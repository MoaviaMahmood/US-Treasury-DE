from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

default_args = {
    "owner": "moavia",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "medallion"],
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command="""
        docker exec spark /opt/spark/bin/spark-submit \
        /opt/spark-apps/spark_jobs/bronze_ingest.py
        """
    )

    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command="""
        docker exec spark /opt/spark/bin/spark-submit \
        /opt/spark-apps/spark_jobs/silver_transform.py
        """
    )

    gold_task = BashOperator(
        task_id="gold_aggregation",
        bash_command="""
        docker exec spark /opt/spark/bin/spark-submit \
        /opt/spark-apps/spark_jobs/gold_aggregations.py
        """
    )

    bronze_task >> silver_task >> gold_task
