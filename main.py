from etl.extract import extract_and_save
from etl.transform import transform_operating_cash_balance, transform_public_debt_transactions
from etl.load import load_operating_cash_balance, load_public_debt_transactions
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from datetime import datetime
# import requests
# import json
# from kafka import KafkaProducer

BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

ENDPOINTS = {
    "operating_cash_balance":"/v1/accounting/dts/operating_cash_balance?filter=record_date:gte:2005-10-03",
    "public_debt_transactions":"/v1/accounting/dts/public_debt_transactions"
}


def run_pipeline():
    try:
        print("Starting extraction...")
        raw_data = extract_and_save(BASE_URL, ENDPOINTS)

        print("Starting transformation...")
        ocb_clean = transform_operating_cash_balance(
            raw_data["operating_cash_balance"]
        )
        pdt_clean = transform_public_debt_transactions(
            raw_data["public_debt_transactions"]
        )

        print("Starting load...")
        load_operating_cash_balance(ocb_clean)
        load_public_debt_transactions(pdt_clean)

        print("Pipeline completed successfully.")

        return ocb_clean, pdt_clean

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()