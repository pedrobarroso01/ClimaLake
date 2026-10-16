import json
import re
import tempfile
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from config import S3_BUCKET, s3_client


NOMES = {
    "bronze": "open_meteo_clima",
    "silver": "clima_silver",
    "gold": "clima_gold",
}

FUSO = ZoneInfo("America/Sao_Paulo")


def data_da_chave(key):
    """Extrai a data de `ingestion_date=AAAA-MM-DD` de uma chave S3."""
    achado = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", key)
    if not achado:
        raise ValueError(
            f"Chave S3 sem 'ingestion_date=AAAA-MM-DD': {key}"
        )

    return date.fromisoformat(achado.group(1))

def excluir_prefixo_s3(cliente_s3, prefix):
    """Exclui os objetos existentes em um prefixo S3."""
    paginador = cliente_s3.get_paginator("list_objects_v2")

    for pagina in paginador.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix,
    ):
        objetos = pagina.get("Contents", [])

        # O S3 permite excluir até 1.000 objetos por requisição.
        for inicio in range(0, len(objetos), 1000):
            lote = objetos[inicio:inicio + 1000]

            cliente_s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={
                    "Objects": [
                        {"Key": objeto["Key"]}
                        for objeto in lote
                    ]
                },
            )


def ingestao_dados(dados, camada, data_ingestao=None):
    """
    Grava dados no S3.

    Bronze: JSON bruto.
    Silver e Gold: DataFrame Spark em Parquet.

    Retorna a referência do armazenamento no S3.
    """
    if camada not in NOMES:
        raise ValueError(f"Camada inválida: {camada}")
    if not S3_BUCKET:
        raise ValueError("Configure S3_BUCKET no arquivo .env")

    data_ingestao = (
        data_ingestao
        or datetime.now(FUSO).date()
    )

    prefix = (
        f"{camada}/"
        f"ingestion_date={data_ingestao:%Y-%m-%d}/"
    )

    cliente_s3 = s3_client()

    # BRONZE: mantém a resposta original da API em JSON.
    if camada == "bronze":
        key = (
            f"{prefix}"
            f"{NOMES[camada]}_{data_ingestao:%Y%m%d}.json"
        )

        cliente_s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(
                dados,
                ensure_ascii=False,
                indent=2,
                default=str,
            ).encode("utf-8"),
            ContentType="application/json",
        )

        return {
            "bucket": S3_BUCKET,
            "key": key,
            "formato": "json",
        }

    # SILVER E GOLD: o Spark gera arquivos Parquet.
    with tempfile.TemporaryDirectory(
        prefix=f"climalake_{camada}_"
    ) as diretorio_temporario:

        caminho_parquet = Path(diretorio_temporario) / "parquet"

        dados.write.mode("overwrite").parquet(
            str(caminho_parquet)
        )

        arquivos_parquet = sorted(
            caminho_parquet.glob("*.parquet")
        )

        if not arquivos_parquet:
            raise ValueError(
                f"Nenhum arquivo Parquet foi gerado para {camada}. "
                "Verifique se o DataFrame contém registros."
            )

        # Remove os arquivos anteriores da mesma camada e data.
        excluir_prefixo_s3(cliente_s3, prefix)

        chaves_s3 = []

        # Envia cada arquivo Parquet para o S3.
        for arquivo in arquivos_parquet:
            nome_arquivo = (
                f"{NOMES[camada]}_"
                f"{data_ingestao:%Y%m%d}_"
                f"{arquivo.name}"
            )

            key = f"{prefix}{nome_arquivo}"

            cliente_s3.upload_file(
                str(arquivo),
                S3_BUCKET,
                key,
            )

            chaves_s3.append(key)

    return {
        "bucket": S3_BUCKET,
        "key": prefix,
        "formato": "parquet",
        "arquivos": chaves_s3,
    }