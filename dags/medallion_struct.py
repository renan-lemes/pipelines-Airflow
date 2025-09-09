from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
from cred.load_cred import load_creds_bq, load_creds_mysql
from dotenv import load_dotenv
import mysql.connector
import os
import re 
import pandas as pd

load_dotenv(dotenv_path='/opt/airflow/dags/.env')

## variaveis iniciais 

BQ_DATALAKE = os.getenv('BQ_DATASET') ## dados que estão no datalake
BQ_BRONZE = os.getenv('BQ_DATASET_BRONZE') ## dados da camada bronze
BQ_SILVER = os.getenv('BQ_DATASET_SILVER')
BQ_GOLD = os.getenv('BQ_DATASET_GOLD')
BQ_MONITORING = os.getenv('BQ_DATASET_LOG') ## dados para salvar os logs dos insets

## conexão com o mysql o banco principal
MYSQL_CONN = {
    'host': 'host.docker.internal',
    'user': os.getenv('USERNAME_MYSQL'),
    'password': os.getenv('PASSWORD_MYSQL'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': os.getenv('PORT_MYSQL')
}

## tabelas bigquery 
BQ_TABLE_ORDERS = 'olist_orders_dataset'
BQ_TABLE_PAYMENT = 'olist_order_payments_dataset'

## tabelas Mysql
MY_SQL_TABLE_CUSTOMERS = 'olist_customers_dataset'
MY_SQL_TABLE_ORDER_ITEMS = 'olist_order_items_dataset'
MY_SQL_TABLE_PRODUCT = 'olist_products_dataset'

## a ordem de orquestração vai ser :  
## 1 - olist_orders_dataset
## 2 - olist_customers_dataset
## 3 - olist_order_payments_dataset
## 4 - olist_order_items_dataset
## 5 - olist_products_dataset

## criar silver tratando os dados como string float e os demais valores
## criando também os joins e as tabelas com os dados transformados.


## definindo a função para dar insert e monitorar as tabelas bronzes
def log_monitoring(pipeline_name, dataset_name, table_name, row_count, status):
    client = bigquery.Client()
    table_id = BQ_MONITORING

    rows_to_insert = [
        {
            "pipeline_name": pipeline_name,
            "dataset_name": dataset_name,
            "table_name": table_name,
            "run_date": datetime.utcnow().isoformat(),
            "row_count": row_count,
            "status": status
        }
    ]
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Erro ao inserir log: {errors}")    

## Carregas as ordens de vendas para o bronze
def orders_bronze(**kwargs):
    client = bigquery.Client()
    query = """
        SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_ORDERS}`
    """
    try:
        df = client.query(query).to_dataframe()
        return df.to_dict(orient='records')
    except Exception as e :
        raise Exception(f'Erro ao extrair do Bigquery: {e}') 



## por fim criamos a dag para rodar todas as pipelines

# with DAG(
#     dag_id='medallion_struct',
#     start_date=datetime(2024,1,1),
#     schedule_interval='0 7-17 * * 1-5',
#     catchup=False,
#     tags=['bronze', 'silver', 'gold', 'BI', 'MEDALLION']
# ) as dag : 