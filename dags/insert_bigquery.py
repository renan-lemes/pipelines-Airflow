from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
from google.cloud import bigquery
from cred.load_cred import load_creds_bq, load_creds_mysql
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='/opt/airflow/dags/.env')

# Carrega credenciais
creds_bq = load_creds_bq()
creds_mysql = load_creds_mysql()

# Configuração de conexão
MYSQL_CONN = {
    'host': os.getenv('HOST_MYSQL'),
    'user': os.getenv('USERNAME_MYSQL'),
    'password': os.getenv('PASSWORD_MYSQL'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': os.getenv('PORT_MYSQL')
}

BQ_PROJECT_ID = creds_bq['BQ_PROJECT_ID']
BQ_DATASET = creds_bq['BQ_DATASET']
BQ_TABLE = 'customers_from_mysql'
table_origem_mysql = 'olist_customers_dataset'

table = 'warehouse'

# Extração do MySQL
def extract_from_mysql(**kwargs):
    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONN)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {table}.{table_origem_mysql}")
        rows = cursor.fetchall()
    except Exception as e:
        raise Exception(f"Erro ao extrair do MySQL: {e}")
    finally:
        cursor.close()
        conn.close()
    return rows

# Criação da tabela no BigQuery
def create_bq_table_if_not_exists(client, dataset_id, table_id, rows):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
    except Exception:
        sample_row = rows[0]
        schema = [bigquery.SchemaField(name, "STRING") for name in sample_row.keys()]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

# Carregamento no BigQuery
def load_to_bigquery(**kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids='extract_mysql')
    if not rows:
        return

    client = bigquery.Client()
    create_bq_table_if_not_exists(client, BQ_DATASET, BQ_TABLE, rows)
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Erro ao inserir no BigQuery: {errors}")

# DAG
with DAG(
    dag_id='mysql_to_bigquery_only',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['mysql', 'bigquery', 'etl']
) as dag:

    extract_mysql = PythonOperator(
        task_id='extract_mysql',
        python_callable=extract_from_mysql
    )

    load_bigquery = PythonOperator(
        task_id='load_bigquery',
        python_callable=load_to_bigquery
    )

    extract_mysql >> load_bigquery
