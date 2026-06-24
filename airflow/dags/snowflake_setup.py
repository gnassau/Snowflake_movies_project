from pathlib import Path
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import snowflake.connector

SNOWFLAKE_CONN_ID = "snowflake_default"
SQL_DIR = Path("/opt/airflow/src/sql/Snowflake_prep")

with DAG(
    dag_id="snowflake_setup",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "setup"],
    doc_md="""
## Snowflake Setup
Executa os scripts SQL de infraestrutura do Snowflake em ordem.
Deve ser executada manualmente uma unica vez antes de iniciar o pipeline.
""",
) as dag:

    create_warehouse = SnowflakeOperator(
        task_id="create_warehouse",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=(SQL_DIR / "00_create_warehouse.sql").read_text(),
    )

    create_database = SnowflakeOperator(
        task_id="create_database",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=(SQL_DIR / "01_create_database.sql").read_text(),
    )

    create_schemas = SnowflakeOperator(
        task_id="create_schemas",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=(SQL_DIR / "02_create_schemas.sql").read_text(),
    )

    create_bronze_table = SnowflakeOperator(
        task_id="create_bronze_tables",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=(SQL_DIR / "03_create_bronze_tables.sql").read_text(),
    )

    # 04 tem placeholders de variaveis ? lido e substituido em runtime
    @task
    def create_storage_integration():
        role_arn  = Variable.get("STORAGE_AWS_ROLE_ARN")
        locations = Variable.get("STORAGE_ALLOWED_LOCATIONS")
        s3_url    = Variable.get("S3_BRONZE_URL")

        allowed = ", ".join(
            f"'{loc.strip()}'" for loc in locations.split(",") if loc.strip()
        )

        sql_template = (SQL_DIR / "04_storage_integration.sql").read_text()
        sql = sql_template.format(
            STORAGE_AWS_ROLE_ARN=role_arn,
            STORAGE_ALLOWED_LOCATIONS=allowed,
            S3_BRONZE_URL=s3_url,
        )

        conn = snowflake.connector.connect(
            user=Variable.get("SNOWFLAKE_USER"),
            password=Variable.get("SNOWFLAKE_PASSWORD"),
            account=Variable.get("SNOWFLAKE_ACCOUNT"),
            warehouse=Variable.get("SNOWFLAKE_WAREHOUSE", default_var="COMPUTE_WH"),
            role=Variable.get("SNOWFLAKE_ROLE", default_var="ACCOUNTADMIN"),
        )
        cur = conn.cursor()
        try:
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt and not stmt.startswith("--"):
                    cur.execute(stmt)
        finally:
            cur.close()
            conn.close()

    (
        create_warehouse
        >> create_database
        >> create_schemas
        >> create_bronze_table
        >> create_storage_integration()
    )
