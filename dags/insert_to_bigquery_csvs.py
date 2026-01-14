from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

CSV_BASE_PATH = "/opt/airflow/dags/data/Bigquery"
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")

def load_csv_to_bigquery(table_name, file_name):
    credentials = service_account.Credentials.from_service_account_file(
        "/opt/airflow/keys/cred.json"
    )

    client = bigquery.Client(
        project=BQ_PROJECT_ID,
        credentials=credentials
    )

    df = pd.read_csv(f"{CSV_BASE_PATH}/{file_name}")

    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )

    client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    ).result()

    print(f"✅ {file_name} carregado em {table_id}")

with DAG(
    dag_id="csv_to_bigquery_simple",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["csv", "bigquery"]
) as dag:

    load_orders = PythonOperator(
        task_id="load_orders_csv",
        python_callable=load_csv_to_bigquery,
        op_kwargs={
            "table_name": "olist_orders_dataset",
            "file_name": "olist_orders_dataset.csv"
        }
    )

    load_payments = PythonOperator(
        task_id="load_payments_csv",
        python_callable=load_csv_to_bigquery,
        op_kwargs={
            "table_name": "olist_order_payments_dataset",
            "file_name": "olist_order_payments_dataset.csv"
        }
    )

    load_orders >> load_payments
