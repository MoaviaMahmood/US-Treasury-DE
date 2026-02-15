from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from etl.pipeline import run_pipeline
from main import run_pipeline

default_args = {
    "owner": "moavia",
    "retries": 2,
}

with DAG(
    dag_id="us_treasury_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    run_etl = PythonOperator(
        task_id="run_full_pipeline",
        python_callable=run_pipeline,
    )
