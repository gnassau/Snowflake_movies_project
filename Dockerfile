FROM apache/airflow:2.9.1

USER root

# Dependências de sistema (importante pra awswrangler e psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && apt-get clean

USER airflow

# Copia requirements para pasta padrão do airflow
COPY requirements.txt /requirements.txt

# Instala dependências
RUN pip install --no-cache-dir -r /requirements.txt