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

load_dotenv(dotenv_path="/opt/airflow/dags/.env")

## variaveis iniciais 

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID") or "your-gcp-project-id" # Substitua 'your-gcp-project-id' pelo ID do seu projeto GCP
BQ_DATALAKE = os.getenv("BQ_DATASET") ## dados que estão no datalake
BQ_BRONZE = os.getenv("BQ_DATASET_BRONZE") ## dados da camada bronze
BQ_SILVER = os.getenv("BQ_DATASET_SILVER")
BQ_GOLD = os.getenv("BQ_DATASET_GOLD")
BQ_MONITORING_DATASET = os.getenv("BQ_DATASET_LOG") or "your_monitoring_dataset" ## dados para salvar os logs dos insets. Substitua 'your_monitoring_dataset' pelo nome do seu dataset de monitoramento.

## conexão com o mysql o banco principal
MYSQL_CONN = {
    "host": "host.docker.internal",
    "user": os.getenv("USERNAME_MYSQL"),
    "password": os.getenv("PASSWORD_MYSQL"),
    "database": os.getenv("MYSQL_DATABASE"),
    "port": os.getenv("PORT_MYSQL")
}

## tabelas bigquery 
BQ_TABLE_ORDERS = "olist_orders_dataset"
BQ_TABLE_PAYMENT = "olist_order_payments_dataset"

## tabelas Mysql
MY_SQL_TABLE_CUSTOMERS = "olist_customers_dataset"
MY_SQL_TABLE_ORDER_ITEMS = "olist_order_items_dataset"
MY_SQL_TABLE_PRODUCT = "olist_products_dataset"

## Funções de monitoramento e log
def log_monitoring(pipeline_name, dataset_name, table_name, row_count, status):
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT_ID}.{BQ_MONITORING_DATASET}.monitoring_log"

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
    
    # Converter a lista de dicionários para um DataFrame do pandas
    df_log = pd.DataFrame(rows_to_insert)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # Anexar ao invés de sobrescrever
        schema=[ # Definir o esquema da tabela de log
            bigquery.SchemaField("pipeline_name", "STRING"),
            bigquery.SchemaField("dataset_name", "STRING"),
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("run_date", "TIMESTAMP"),
            bigquery.SchemaField("row_count", "INTEGER"),
            bigquery.SchemaField("status", "STRING"),
        ],
    )

    try:
        job = client.load_table_from_dataframe(df_log, table_id, job_config=job_config)
        job.result() # Espera o job terminar
        print(f"Log inserido com sucesso na tabela {table_id}")
    except Exception as e:
        print(f"Erro ao inserir log na tabela {table_id}: {e}")

## Funções para a camada Bronze
def extract_from_bigquery_to_bronze(table_name, **kwargs):
    client = bigquery.Client()
    source_table_id = f"{BQ_PROJECT_ID}.{BQ_DATALAKE}.{table_name}"
    destination_table_id = f"{BQ_PROJECT_ID}.{BQ_BRONZE}.{table_name}"

    try:
        query = f"SELECT * FROM `{source_table_id}`"
        df = client.query(query).to_dataframe()
        
        # Carregar para a camada Bronze
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = client.load_table_from_dataframe(df, destination_table_id, job_config=job_config)
        job.result() # Espera o job terminar

        row_count = df.shape[0]
        log_monitoring(f"bronze_{table_name}", BQ_BRONZE, table_name, row_count, "SUCCESS")
        print(f"Dados da tabela {table_name} carregados para Bronze. Total de linhas: {row_count}")

    except Exception as e:
        log_monitoring(f"bronze_{table_name}", BQ_BRONZE, table_name, 0, f"FAILED: {e}")
        raise Exception(f"Erro ao extrair/carregar {table_name} para Bronze: {e}")

def extract_from_mysql_to_bronze(table_name, **kwargs):
    try:
        conn = mysql.connector.connect(**MYSQL_CONN)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)
        conn.close()

        client = bigquery.Client()
        destination_table_id = f"{BQ_PROJECT_ID}.{BQ_BRONZE}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = client.load_table_from_dataframe(df, destination_table_id, job_config=job_config)
        job.result() # Espera o job terminar

        row_count = df.shape[0]
        log_monitoring(f"bronze_{table_name}", BQ_BRONZE, table_name, row_count, "SUCCESS")
        print(f"Dados da tabela {table_name} carregados para Bronze. Total de linhas: {row_count}")

    except Exception as e:
        log_monitoring(f"bronze_{table_name}", BQ_BRONZE, table_name, 0, f"FAILED: {e}")
        raise Exception(f"Erro ao extrair/carregar {table_name} para Bronze: {e}")

## Funções para a camada Silver
def transform_to_silver(**kwargs):
    client = bigquery.Client()
    silver_table_id = f"{BQ_PROJECT_ID}.{BQ_SILVER}.olist_silver_dataset"

    # A query comentada no código original será a base para a camada Silver
    # Ajustando para usar os datasets Bronze e garantir a tipagem correta
    query = f"""
        SELECT 
            O.order_id, O.customer_id, O.order_status, O.order_purchase_timestamp, O.order_approved_at,
            O.order_delivered_carrier_date, O.order_delivered_customer_date, O.order_estimated_delivery_date,
            C.customer_unique_id, C.customer_zip_code_prefix, C.customer_city, C.customer_state,
            P.payment_sequential, P.payment_type, P.payment_installments, P.payment_value,
            I.order_item_id, I.product_id, I.seller_id, I.shipping_limit_date, I.price, I.freight_value,
            PR.product_category_name, PR.product_name_lenght, PR.product_description_lenght,
            PR.product_photos_qty, PR.product_weight_g, PR.product_length_cm,
            PR.product_height_cm, PR.product_width_cm
        FROM `{BQ_PROJECT_ID}.{BQ_BRONZE}.{BQ_TABLE_ORDERS}` AS O
        LEFT JOIN `{BQ_PROJECT_ID}.{BQ_BRONZE}.{MY_SQL_TABLE_CUSTOMERS}` AS C
               ON O.customer_id = C.customer_id
        LEFT JOIN `{BQ_PROJECT_ID}.{BQ_BRONZE}.{BQ_TABLE_PAYMENT}` AS P
               ON O.order_id = P.order_id
        LEFT JOIN `{BQ_PROJECT_ID}.{BQ_BRONZE}.{MY_SQL_TABLE_ORDER_ITEMS}` AS I
               ON O.order_id = I.order_id
        LEFT JOIN `{BQ_PROJECT_ID}.{BQ_BRONZE}.{MY_SQL_TABLE_PRODUCT}` AS PR
               ON I.product_id = PR.product_id
    """

    try:
        job_config = bigquery.QueryJobConfig(destination=silver_table_id)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        query_job = client.query(query, job_config=job_config)
        query_job.result() # Espera o job terminar

        # Obter o número de linhas inseridas
        destination_table = client.get_table(silver_table_id)
        row_count = destination_table.num_rows

        log_monitoring("silver_transformation", BQ_SILVER, "olist_silver_dataset", row_count, "SUCCESS")
        print(f"Dados transformados e carregados para Silver. Total de linhas: {row_count}")

    except Exception as e:
        log_monitoring("silver_transformation", BQ_SILVER, "olist_silver_dataset", 0, f"FAILED: {e}")
        raise Exception(f"Erro ao transformar/carregar para Silver: {e}")

## Funções para a camada Gold
def transform_to_gold(**kwargs):
    client = bigquery.Client()
    gold_table_id = f"{BQ_PROJECT_ID}.{BQ_GOLD}.olist_gold_summary"
    silver_table_id = f"{BQ_PROJECT_ID}.{BQ_SILVER}.olist_silver_dataset"

    # Exemplo de agregação para a camada Gold: vendas por estado e tipo de pagamento
    query = f"""
        SELECT
            customer_state,
            payment_type,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(payment_value) AS total_payment_value,
            AVG(payment_installments) AS avg_installments
        FROM `{silver_table_id}`
        GROUP BY customer_state, payment_type
        ORDER BY total_payment_value DESC
    """

    try:
        job_config = bigquery.QueryJobConfig(destination=gold_table_id)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        
        query_job = client.query(query, job_config=job_config)
        query_job.result() # Espera o job terminar

        # Obter o número de linhas inseridas
        destination_table = client.get_table(gold_table_id)
        row_count = destination_table.num_rows

        log_monitoring("gold_transformation", BQ_GOLD, "olist_gold_summary", row_count, "SUCCESS")
        print(f"Dados transformados e carregados para Gold. Total de linhas: {row_count}")

    except Exception as e:
        log_monitoring("gold_transformation", BQ_GOLD, "olist_gold_summary", 0, f"FAILED: {e}")
        raise Exception(f"Erro ao transformar/carregar para Gold: {e}")

default_args = {
    'owner': 'Renan Lemes Leepkaln',
    'depends_on_past': False,
    'retries': 0
}

## Definição da DAG
with DAG(
    dag_id="medallion_struct_improved",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 7-17 * * 1-5", # Rodar de segunda a sexta, das 7h às 17h, a cada hora
    catchup=False,
    tags=["bronze", "silver", "gold", "BI", "MEDALLION", "IMPROVED"],
    default_args=default_args
) as dag:

    # Tarefas da camada Bronze (Extração e Carregamento)
    extract_orders_to_bronze = PythonOperator(
        task_id="extract_orders_to_bronze",
        python_callable=extract_from_bigquery_to_bronze,
        op_kwargs={"table_name": BQ_TABLE_ORDERS},
    )

    extract_payments_to_bronze = PythonOperator(
        task_id="extract_payments_to_bronze",
        python_callable=extract_from_bigquery_to_bronze,
        op_kwargs={"table_name": BQ_TABLE_PAYMENT},
    )

    extract_customers_to_bronze = PythonOperator(
        task_id="extract_customers_to_bronze",
        python_callable=extract_from_mysql_to_bronze,
        op_kwargs={"table_name": MY_SQL_TABLE_CUSTOMERS},
    )

    extract_order_items_to_bronze = PythonOperator(
        task_id="extract_order_items_to_bronze",
        python_callable=extract_from_mysql_to_bronze,
        op_kwargs={"table_name": MY_SQL_TABLE_ORDER_ITEMS},
    )

    extract_products_to_bronze = PythonOperator(
        task_id="extract_products_to_bronze",
        python_callable=extract_from_mysql_to_bronze,
        op_kwargs={"table_name": MY_SQL_TABLE_PRODUCT},
    )

    # Tarefa da camada Silver (Transformação e Carregamento)
    transform_silver_layer = PythonOperator(
        task_id="transform_silver_layer",
        python_callable=transform_to_silver,
    )

    # Tarefa da camada Gold (Transformação e Carregamento)
    transform_gold_layer = PythonOperator(
        task_id="transform_gold_layer",
        python_callable=transform_to_gold,
    )

    # Definição da ordem das tarefas
    [extract_orders_to_bronze, extract_payments_to_bronze, extract_customers_to_bronze, extract_order_items_to_bronze, extract_products_to_bronze] >> transform_silver_layer >> transform_gold_layer

