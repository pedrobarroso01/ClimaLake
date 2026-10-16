"""Download dos objetos das camadas Bronze/Silver do S3."""

import shutil
from pathlib import Path, PurePosixPath

from config import s3_client

# Diretório raiz do projeto ClimaLake.
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"

CAMADAS_IMPORTAVEIS = ("bronze", "silver")


def importar_dados(arquivo: dict, camada: str) -> str:
    """
    Baixa os dados do S3 para a pasta local.

    Bronze: baixa um arquivo JSON.
    Silver: baixa os arquivos Parquet da partição.

    Retorna o caminho local do arquivo JSON ou da pasta Parquet.
    """
    if camada not in CAMADAS_IMPORTAVEIS:
        raise ValueError("A camada deve ser 'bronze' ou 'silver'.")

    if not isinstance(arquivo, dict):
        raise TypeError("A referência S3 deve ser um dicionário.")

    bucket = arquivo.get("bucket")
    key = arquivo.get("key")

    if not bucket or not key:
        raise ValueError(
            "A referência S3 deve conter os campos 'bucket' e 'key'."
        )

    cliente = s3_client()

    # BRONZE: download de um único objeto JSON.
    if camada == "bronze":
        diretorio = DATA_DIR / camada
        diretorio.mkdir(parents=True, exist_ok=True)

        caminho_local = diretorio / PurePosixPath(key).name

        cliente.download_file(
            bucket,
            key,
            str(caminho_local),
        )

        return str(caminho_local)

    # SILVER: download de todos os arquivos Parquet da partição.
    arquivos = arquivo.get("arquivos", [])

    # Caso a lista não tenha sido fornecida, busca os arquivos pelo prefixo.
    if not arquivos:
        prefixo = key.rstrip("/") + "/"
        paginador = cliente.get_paginator("list_objects_v2")

        arquivos = [
            objeto["Key"]
            for pagina in paginador.paginate(
                Bucket=bucket,
                Prefix=prefixo,
            )
            for objeto in pagina.get("Contents", [])
            if objeto["Key"].endswith(".parquet")
        ]

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo Parquet encontrado para a referência: {key}"
        )

    # Mantém os dados separados por data de ingestão.
    nome_particao = PurePosixPath(key.rstrip("/")).name
    diretorio_local = DATA_DIR / camada / nome_particao

    # Evita misturar arquivos de uma execução anterior da mesma partição.
    if diretorio_local.exists():
        shutil.rmtree(diretorio_local)

    diretorio_local.mkdir(parents=True, exist_ok=True)

    for chave_s3 in arquivos:
        caminho_local = diretorio_local / PurePosixPath(chave_s3).name

        cliente.download_file(
            bucket,
            chave_s3,
            str(caminho_local),
        )

    return str(diretorio_local)