from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError
from cred.load_cred import load_creds_bq, load_creds_mysql
from dotenv import load_dotenv
import mysql.connector
import os
import re
import pandas as pd

load_dotenv(dotenv_path='/opt/airflow/dags/.env')

# Carrega credenciais
creds_bq = load_creds_bq()
creds_mysql = load_creds_mysql()

# Configuração de conexão
MYSQL_CONN = {
    'host': 'host.docker.internal',
    'user': os.getenv('USERNAME_MYSQL'),
    'password': os.getenv('PASSWORD_MYSQL'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': os.getenv('PORT_MYSQL')
}

BQ_PROJECT_ID = creds_bq['BQ_PROJECT_ID']
BQ_DATASET = creds_bq['BQ_DATASET']
BQ_TABLE = 'products_from_mysql'
TABLE_ORIGEM_MYSQL = 'olist_products_dataset'
SCHEMA_MYSQL = 'warehouse'

## pra transformar espaço em um nome como _ que a API do BIGQUERY não permite espaço
def sanitize_column(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

# Extração do MySQL
def extract_from_mysql(table_name, **kwargs):
    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONN)
        cursor = conn.cursor(dictionary=True)
        query = f"SELECT * FROM {SCHEMA_MYSQL}.{table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()
    except Exception as e:
        raise Exception(f"Erro ao extrair do MySQL: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return rows

# Truncate da tabela no BigQuery
def truncate_bq_table(client, dataset_id, table_id):
    query = f"DELETE FROM `{BQ_PROJECT_ID}.{dataset_id}.{table_id}` WHERE TRUE"
    query_job = client.query(query)
    query_job.result()
    print(f"Tabela {dataset_id}.{table_id} truncada com sucesso.")

## Criação da tabela no BigQuery caso não exista
def create_bq_table_if_not_exists(client, dataset_id, table_id, rows):
    if not rows:
        raise ValueError("Nenhuma linha retornada do MySQL para criação da tabela no BigQuery.")

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        print(f"Tabela {dataset_id}.{table_id} já existe no BigQuery.")
    except GoogleAPIError:
        print(f"Tabela {dataset_id}.{table_id} não encontrada. Será criada.")
        sample_row = rows[0]
        schema = [bigquery.SchemaField(sanitize_column(name), "STRING") for name in sample_row.keys()] ## for para inserir a quantidade de colunas em STRING para o bigquery
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Tabela {dataset_id}.{table_id} criada com sucesso.")

# Carregamento no BigQuery
def load_to_bigquery(table_name, **kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids='extract_mysql')
    if not rows:
        print("Nenhum dado para inserir no BigQuery.")
        return

    df = pd.DataFrame(rows)
    df = df.astype(str)  # converte tudo para string

    client = bigquery.Client()
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    ## cria a tabela com o schema pronto 
    create_bq_table_if_not_exists(client, BQ_DATASET, table_name, rows)

    ## executa o job para inserir os dados 
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    print(f"{job.output_rows} linhas inseridas com sucesso no BigQuery.")

## argumentos para o job
default_args = {
    'owner':'Renan Lemes Leepkaln',
    'depends_on_past':False,
    'retries':0
}

# DAG
with DAG(
    dag_id='mysql_to_bigquery_truncate_insert',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['mysql', 'bigquery', 'truncate-insert', 'products']
) as dag:

    extract_mysql = PythonOperator(
        task_id='extract_mysql',
        python_callable=extract_from_mysql,
        op_kwargs={'table_name': TABLE_ORIGEM_MYSQL}  
    )

    load_bigquery = PythonOperator(
        task_id='load_bigquery',
        python_callable=load_to_bigquery,
        op_kwargs={'table_name': TABLE_ORIGEM_MYSQL}  
    )

    extract_mysql >> load_bigquery