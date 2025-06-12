from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime
import os

# Define a função que lista as tabelas
def list_bq_tables():
    # Se quiser, imprima o caminho da credencial (opcional para debug)
    print("🔐 Usando credencial:", os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))

    # Cria o cliente BigQuery
    client = bigquery.Client()

    # Define o projeto e dataset (ajuste com os seus valores)
    project_id = os.getenv("BQ_PROJECT_ID")  
    dataset_id = os.getenv("BQ_DATASET")     

    # Cria referência ao dataset
    dataset_ref = client.dataset(dataset_id, project=project_id)

    # Lista as tabelas
    tables = list(client.list_tables(dataset_ref))

    if tables:
        print(f"📦 Tabelas encontradas no dataset {dataset_id}:")
        for table in tables:
            print(f" - {table.table_id}")
    else:
        print("⚠️ Nenhuma tabela encontrada.")

# Define o DAG
with DAG(
    dag_id='list_bigquery_tables_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "básico"],
) as dag:

    list_tables_task = PythonOperator(
        task_id='list_bigquery_tables',
        python_callable=list_bq_tables
    )
