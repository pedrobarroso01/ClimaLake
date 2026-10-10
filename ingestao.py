import json
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

from config import S3_BUCKET, s3_client

NOMES = {"bronze": "open_meteo_clima", "silver": "clima_silver", "gold": "clima_gold"}

FUSO = ZoneInfo("America/Sao_Paulo")


def data_da_chave(key):
    """Extrai a data de `ingestion_date=AAAA-MM-DD` de uma chave S3."""
    achado = re.search(r"ingestion_date=(\d{4}-\d{2}-\d{2})", key)
    if not achado:
        raise ValueError(f"Chave S3 sem 'ingestion_date=AAAA-MM-DD': {key}")
    return date.fromisoformat(achado.group(1))


def ingestao_dados(dados, camada, data_ingestao=None):
    """Grava `dados` no S3 e devolve a referência {'bucket', 'key', 'formato'}.

    - Bronze recebe o JSON bruto (dict); 
      Silver e Gold recebem um DataFrame Spark.
    
    - A chave é determinística por dia (sem horário no nome): reexecutar no mesmo
      dia sobrescreve o objeto em vez de duplicar. 
    - `data_ingestao` permite que Silver e Gold reutilizem a data do Bronze, mesmo
      que a execução atravesse a meia-noite.

    """
    if camada not in NOMES:
        raise ValueError(f"Camada inválida: {camada}")
    if not S3_BUCKET:
        raise ValueError("Configure S3_BUCKET no arquivo .env")

    data_ingestao = data_ingestao or datetime.now(FUSO).date()

    if camada == "bronze":
        registros = dados
    else:
        registros = [linha.asDict(recursive=True) for linha in dados.collect()]

    key = (
        f"{camada}/ingestion_date={data_ingestao:%Y-%m-%d}/"
        f"{NOMES[camada]}_{data_ingestao:%Y%m%d}.json"
    )

    s3_client().put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(registros, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )

    return {"bucket": S3_BUCKET, "key": key, "formato": "json"}
