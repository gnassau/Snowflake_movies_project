from airflow import DAG
from datetime import datetime
from pathlib import Path
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator

from src.Bronze.full_load import run_ingestion
from src.Bronze.get_updated_ids import run_get_updated_ids
from src.Bronze.update_movies import run_get_movie_details

SNOWFLAKE_CONN_ID = "snowflake_default"
DBT_DIR = "/opt/airflow/movies_dbt"

with DAG(
    dag_id="snowflake_ingest",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath="/opt/airflow/src/sql",
    params={
        "load_type": Param(
            "incremental_refresh",
            type="string",
            enum=["incremental_refresh", "full_refresh"],
            description="Tipo de ingestao",
        ),
        "start_date": Param(
            None,
            type=["null", "string"],
            description="Data inicial YYYY-MM-DD",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            description="Data final YYYY-MM-DD",
        ),
    },
) as dag:

    # =========================
    # BRONZE
    # =========================

    with TaskGroup("bronze_layer") as bronze:

        @task
        def full_load(**context):
            params = context["params"]
            run_ingestion(
                mode=params["load_type"],
                start_date=params["start_date"],
                end_date=params["end_date"],
            )

        @task
        def get_updated_ids(**context):
            params = context["params"]
            run_get_updated_ids(
                mode=params["load_type"],
                start_date=params["start_date"],
                end_date=params["end_date"],
            )

        @task
        def get_movie_details():
            run_get_movie_details()

        full_load() >> get_updated_ids() >> get_movie_details()

    # =========================
    # COPY INTO SNOWFLAKE
    # =========================

    copy_into_snowflake = SnowflakeOperator(
        task_id="copy_into_snowflake",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="/Snowflake_prep/05_copy_into.sql"
    )


    bronze >> copy_into_snowflake
