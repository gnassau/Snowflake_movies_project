from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["teste", "snowflake"],
) as dag:

    test_connection = SnowflakeOperator(
        task_id="test_connection",
        snowflake_conn_id="snowflake_default",  # Connection Id criado no Airflow
        sql="SELECT 1;",
    )

    test_connection