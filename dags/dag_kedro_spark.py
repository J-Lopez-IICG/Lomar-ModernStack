from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="ejecutar_pipeline_kedro",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Ejecución manual por ahora
    catchup=False,
    tags=["produccion", "kedro"],
) as dag:

    # En tu archivo dag_kedro_spark.py
    run_kedro = BashOperator(
        task_id="run_kedro_spark",
        # Añadimos DOCKER_API_VERSION justo antes del comando
        bash_command="export DOCKER_API_VERSION=1.44 && docker exec lomar_spark_env kedro run",
    )
