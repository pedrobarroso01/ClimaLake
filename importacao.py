"""Download dos objetos das camadas Bronze/Silver do S3 para a pasta local."""

from pathlib import Path

from config import s3_client

# Diretório raiz do projeto ClimaLake.
PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"

CAMADAS_IMPORTAVEIS = ("bronze", "silver")


def importar_dados(arquivo: dict, camada: str) -> str:
    """
    Baixa o objeto S3 indicado em arquivo ({'bucket', 'key'})
    para a pasta local da camada e devolve o caminho do arquivo.
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

    diretorio = DATA_DIR / camada
    diretorio.mkdir(parents=True, exist_ok=True)

    caminho_local = diretorio / Path(key).name

    s3_client().download_file(
        bucket,
        key,
        str(caminho_local),
    )

    return str(caminho_local)
