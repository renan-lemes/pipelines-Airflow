from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
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
BQ_TABLE = 'customers_from_mysql'
table_origem_mysql = 'olist_customers_dataset'

table = 'warehouse'

def sanitize_column(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

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

# Criação da tabela no BigQuery caso não exista
def create_bq_table_if_not_exists(client, dataset_id, table_id, rows):
    if not rows:
        raise ValueError("Nenhuma linha retornada do MySQL para criação da tabela no BigQuery.")

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        print(f"Tabela {dataset_id}.{table_id} já existe no BigQuery.")
    except GoogleAPIError as e:
        print(f"Tabela {dataset_id}.{table_id} não encontrada. Será criada.")

        # Usa a primeira linha para inferir o schema
        sample_row = rows[0]

        schema = [
            bigquery.SchemaField(sanitize_column(name), "STRING")
            for name in sample_row.keys()
        ]

        # Criação da tabela com schema sanitizado
        table = bigquery.Table(table_ref, schema=schema)

        try:
            client.create_table(table)
            print(f"Tabela {dataset_id}.{table_id} criada com sucesso no BigQuery.")
        except GoogleAPIError as create_err:
            raise Exception(f"Erro ao criar tabela no BigQuery: {create_err.message}")
        
# Carregamento no BigQuery
def load_to_bigquery(**kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids='extract_mysql')
    if not rows:
        print("Nenhum dado para inserir no BigQuery.")
        return

    df = pd.DataFrame(rows)

    # transformando as colunas em str
    cols = df.columns
    for col in cols:
        df[col] = df[col].astype(str)

    client = bigquery.Client()
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Aguarda a conclusão

    print(f"{job.output_rows} linhas inseridas com sucesso no BigQuery.")

# DAG
with DAG(
    dag_id='mysql_to_bigquery_only',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, ## aqui posso adicionar se e diario ou a forma que preferir exemplo '0 7-17 * * 1-5'
    catchup=False,
    tags=['mysql', 'bigquery', 'raw']
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
