FROM apache/airflow:3.3.2

USER root

# Instala o Java 17, necessário para o PySpark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copia as dependências Python do projeto.
COPY requirements.txt /requirements.txt

# Fixa a versão do Airflow para o pip não trocá-la ao resolver as dependências
# (o provider FAB já está em requirements.txt).
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    -r /requirements.txt
