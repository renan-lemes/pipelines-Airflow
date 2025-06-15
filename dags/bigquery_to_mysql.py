from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from cred.load_cred import load_creds_bq, load_creds_mysql
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
import os

load_dotenv(dotenv_path='/opt/airflow/dags/.env')

# Carregar credenciais
creds_bq = load_creds_bq()
creds_mysql = load_creds_mysql()

# Configurações
BQ_PROJECT_ID = creds_bq['BQ_PROJECT_ID']
BQ_DATASET = creds_bq['BQ_DATASET']
BQ_TABLE = 'customers_from_mysql'

TABLE_DESTINO_MYSQL = 'olist_order_items_dataset'
SCHEMA_MYSQL = 'warehouse'

MYSQL_CONN = {
    'host': 'host.docker.internal',
    'user': os.getenv('USERNAME_MYSQL'),
    'password': os.getenv('PASSWORD_MYSQL'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': os.getenv('PORT_MYSQL')
}

# Extração do BigQuery
def extract_from_bigquery(**kwargs):
    client = bigquery.Client()
    query = f"""
        SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    """
    try:
        df = client.query(query).to_dataframe()
        return df.to_dict(orient='records')
    except Exception as e:
        raise Exception(f"Erro ao extrair do BigQuery: {e}")

# Carga no MySQL
def load_to_mysql(**kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids='extract_bigquery')
    if not rows:
        print("Nenhum dado retornado do BigQuery para inserir no MySQL.")
        return

    df = pd.DataFrame(rows)

    conn = mysql.connector.connect(**MYSQL_CONN)
    cursor = conn.cursor()

    # Criação da tabela se não existir
    columns = df.columns
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_MYSQL}.{TABLE_DESTINO_MYSQL} (
        {', '.join(f"{col} TEXT" for col in columns)}
    );
    """
    try:
        cursor.execute(create_table_sql)
    except Exception as e:
        raise Exception(f"Erro ao criar a tabela no MySQL: {e}")

    # Inserção dos dados
    insert_sql = f"""
    INSERT INTO {SCHEMA_MYSQL}.{TABLE_DESTINO_MYSQL} ({', '.join(columns)})
    VALUES ({', '.join(['%s'] * len(columns))});
    """
    values = [tuple(map(str, row)) for row in df.values]

    try:
        cursor.executemany(insert_sql, values)
        conn.commit()
        print(f"{cursor.rowcount} linhas inseridas com sucesso no MySQL.")
    except Exception as e:
        conn.rollback()
        raise Exception(f"Erro ao inserir dados no MySQL: {e}")
    finally:
        cursor.close()
        conn.close()

# DAG
with DAG(
    dag_id='bigquery_to_mysql_only',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # ou '0 7-17 * * 1-5' para rodar das 7h às 17h de seg a sex
    catchup=False,
    tags=['mysql', 'bigquery', 'raw']
) as dag:

    extract_bigquery = PythonOperator(
        task_id='extract_bigquery',
        python_callable=extract_from_bigquery
    )

    load_mysql = PythonOperator(
        task_id='load_mysql',
        python_callable=load_to_mysql
    )

    extract_bigquery >> load_mysql
