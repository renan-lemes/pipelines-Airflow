from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import mysql.connector
import pandas as pd
import os
import re
from dotenv import load_dotenv
from cred.load_cred import load_creds_bq, load_creds_mysql
from airflow.utils.log.logging_mixin import LoggingMixin

load_dotenv(dotenv_path='/opt/airflow/dags/.env')

## loger do airflow
log = LoggingMixin().log 


creds_bq = load_creds_bq()
creds_mysql = load_creds_mysql()

MYSQL_CONN = {
    'host': 'host.docker.internal',
    'user': os.getenv('USERNAME_MYSQL'),
    'password': os.getenv('PASSWORD_MYSQL'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': os.getenv('PORT_MYSQL')
}

BQ_PROJECT_ID = creds_bq['BQ_PROJECT_ID']
BQ_DATASET = creds_bq['BQ_DATASET']
MYSQL_SCHEMA = 'warehouse'

# Lista das tabelas
TABLES = ['olist_geolocation_dataset', 'olist_order_reviews_dataset', 'olist_order_items_dataset']

# Conta linhas para ordenar da maior para a menor
def get_tables_ordered_by_size():
    conn = mysql.connector.connect(**MYSQL_CONN)
    cursor = conn.cursor()
    table_counts = []

    for table in TABLES:
        cursor.execute(f"SELECT COUNT(*) FROM {MYSQL_SCHEMA}.{table}")
        count = cursor.fetchone()[0]
        table_counts.append((table, count))

    cursor.close()
    conn.close()

    ## Ordena pela contagem de forma decrescente
    sorted_tables = sorted(table_counts, key=lambda x: x[1], reverse=True)
    return [table for table, _ in sorted_tables]

def sanitize_column(name):
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def extract_from_mysql(table_name, **kwargs):
    log.info(f"Iniciado extração da tabela {table_name} do Mysql")
    try:
        conn = mysql.connector.connect(**MYSQL_CONN)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {MYSQL_SCHEMA}.{table_name}")
        rows = cursor.fetchall()
    except Exception as e:
        raise Exception(f"Erro ao extrair do MySQL: {e}")
    finally:
        cursor.close()
        conn.close()
    return rows

def create_bq_table_if_not_exists(client, dataset_id, table_id, rows):
    log.info("-----------------------------------------------------------------------")
    log.info(f"Verificando se a tabela {dataset_id}.{table_id} existe no BigQuery ...")
    log.info("-----------------------------------------------------------------------")

    if not rows:
        raise ValueError("Nenhuma linha para criação da tabela.")
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        log.info("-----------------------------------------------------")
        log.info(f"     Tabela {dataset_id}.{table_id} já existe       ")
        log.info("-----------------------------------------------------")
    except GoogleAPIError:
        log.info("-----------------------------------------------------")
        log.warning(f"Tabela {dataset_id}.{table_id} não existe criando ...")
        log.info("-----------------------------------------------------")
        
        ## monta o schema com as colunas
        schema = [bigquery.SchemaField(sanitize_column(k), "STRING") for k in rows[0].keys()]
        table = bigquery.Table(table_ref, schema=schema)
        ## cria a tabela 
        client.create_table(table)
        log.info("-----------------------------------------------------")
        log.info(f"Tabela {dataset_id}.{table_id} criada com sucesso !!")
        log.info("-----------------------------------------------------")


def load_to_bigquery(table_name, **kwargs):
    log.info("---------------------------------------------------------")
    log.info(f"Iniciado a caraga para o BigQuery da tabela {table_name}")
    log.info("---------------------------------------------------------")

    rows = kwargs['ti'].xcom_pull(task_ids=f'extract_mysql_{table_name}')
    if not rows:
        log.info("---------------------------------------------------------")
        log.warning(f"Nenhum dado encontrado para a tabela {table_name}")
        log.info("---------------------------------------------------------")
        return
    ## transforma os dados em STR
    df = pd.DataFrame(rows).astype(str)
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    create_bq_table_if_not_exists(client, BQ_DATASET, table_name, rows)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    log.info("---------------------------------------------------------")
    log.info(f"Carga concluída: {job.output_rows} linhas inseridas em {table_name}.")
    log.info("---------------------------------------------------------")
    
default_args = {
    'owner': 'Renan Lemes Leepkaln',
    'depends_on_past': False,
    'retries': 0
}

# DAG unica com tarefas para cada tabela, ordenadas por volume
with DAG(
    dag_id='mysql_to_bigquery_all_tables',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['mysql', 'bigquery', 'stageTodos'],
    default_args=default_args
) as dag:
    ## retorna a lista de ordenado das tabelas em ordem do maior para o menor a quantidade de linhas que tem na tabela 
    ordered_tables = get_tables_ordered_by_size()

    for table in ordered_tables:
        extract_task = PythonOperator(
            task_id=f'extract_mysql_{table}',
            python_callable=extract_from_mysql,
            op_kwargs={'table_name': table}
        )

        load_task = PythonOperator(
            task_id=f'load_bigquery_{table}',
            python_callable=load_to_bigquery,
            op_kwargs={'table_name': table}
        )

        extract_task >> load_task
