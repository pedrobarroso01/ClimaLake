import logging
import os
import sys
from pathlib import Path
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

logger = logging.getLogger("airflow.task")

# Caminho do projeto dentro dos containers Docker.
PROJECT_DIR = Path("/opt/airflow/project")


def preparar_ambiente():
    if not PROJECT_DIR.exists():
        raise FileNotFoundError(
            f"Pasta do projeto não encontrada: {PROJECT_DIR}"
        )

    # Define o diretório de trabalho.
    os.chdir(PROJECT_DIR)

    # Permite importar os módulos do projeto.
    project_path = str(PROJECT_DIR)

    if project_path not in sys.path:
        sys.path.insert(0, project_path)


def validar_referencia_s3(arquivo, camada):
    """
    Confirma que a ingestão retornou uma referência S3
    contendo o bucket e a chave do objeto.
    """
    if not isinstance(arquivo, dict):
        raise TypeError(
            f"A camada {camada} deveria retornar um dicionário "
            f"com 'bucket' e 'key', mas retornou "
            f"{type(arquivo).__name__}."
        )

    if not arquivo.get("bucket") or not arquivo.get("key"):
        raise ValueError(
            f"Referência S3 inválida na camada {camada}: "
            "faltam os campos 'bucket' ou 'key'."
        )

    return arquivo


@dag(
    dag_id="climalake_pipeline",
    description="Pipeline de dados climáticos: Bronze, Silver e Gold",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["climalake", "etl", "pyspark", "aws"],
)
def climalake_pipeline():

    @task
    def bronze() -> dict:
        """Extrai os dados climáticos e realiza a ingestão Bronze."""
        preparar_ambiente()

        from extracao import extracao_dados
        from ingestao import ingestao_dados

        logger.info("Iniciando camada Bronze...")

        dados = extracao_dados()
        logger.info("Extração dos dados concluída")

        arquivo_bronze = ingestao_dados(dados, "bronze")
        arquivo_bronze = validar_referencia_s3(
            arquivo_bronze, "bronze"
        )

        logger.info("Ingestão Bronze concluída")
        logger.info("Objeto Bronze: %s", arquivo_bronze["key"])

        # Retorna o dicionário para o XCom, sem convertê-lo em string.
        return arquivo_bronze

    @task
    def silver(arquivo_bronze: dict) -> dict:
        """Importa Bronze, transforma os dados e realiza a ingestão Silver."""
        preparar_ambiente()

        from importacao import importar_dados
        from transform import transformar_silver
        from ingestao import ingestao_dados

        logger.info("Iniciando camada Silver...")

        # Recebe a referência S3 como dicionário.
        dados_importados = importar_dados(
            arquivo_bronze,
            "bronze",
        )

        transformacoes_silver = transformar_silver(
            dados_importados
        )
        logger.info("Transformação Silver concluída")

        arquivo_silver = ingestao_dados(
            transformacoes_silver,
            "silver",
        )
        arquivo_silver = validar_referencia_s3(
            arquivo_silver, "silver"
        )

        logger.info("Ingestão Silver concluída")
        logger.info("Objeto Silver: %s", arquivo_silver["key"])

        # Preserva o dicionário para a tarefa Gold.
        return arquivo_silver

    @task
    def gold(arquivo_silver: dict) -> dict:
        """Importa Silver, calcula indicadores e realiza a ingestão Gold."""
        preparar_ambiente()

        from importacao import importar_dados
        from transform import transformar_gold
        from ingestao import ingestao_dados

        logger.info("Iniciando camada Gold...")

        dados_importados = importar_dados(
            arquivo_silver,
            "silver",
        )

        transformacoes_gold = transformar_gold(
            dados_importados
        )
        logger.info("Transformação Gold concluída")

        arquivo_gold = ingestao_dados(
            transformacoes_gold,
            "gold",
        )
        arquivo_gold = validar_referencia_s3(
            arquivo_gold, "gold"
        )

        logger.info("Ingestão Gold concluída")
        logger.info("Objeto Gold: %s", arquivo_gold["key"])
        logger.info("Pipeline ClimaLake finalizado com sucesso")

        return arquivo_gold

    # Dependências: Bronze -> Silver -> Gold.
    arquivo_bronze = bronze()
    arquivo_silver = silver(arquivo_bronze)
    gold(arquivo_silver)


climalake_pipeline()