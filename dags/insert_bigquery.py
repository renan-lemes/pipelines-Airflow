from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
from google.cloud import bigquery

# Configurações
MYSQL_CONN = {
    'host': 'mysql',
    'user': 'airflow',
    'password': 'airflow',
    'database': 'my_database'
}

BQ_PROJECT_ID = 'seu-projeto-gcp'
BQ_DATASET = 'seu_dataset'
BQ_TABLE = 'tabela_destino'

# Função para extrair dados do MySQL
def extract_from_mysql(**kwargs):
    conn = mysql.connector.connect(**MYSQL_CONN)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tabela_origem")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    # Salva no XCom
    return rows

# Função para criar a tabela no BigQuery, caso ela não exista
def create_bq_table_if_not_exists(client, dataset_id, table_id, rows):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        print("Tabela já existe no BigQuery.")
    except Exception:
        print("Tabela não existe. Criando agora...")

        # Cria o schema a partir dos dados do MySQL
        sample_row = rows[0]
        schema = [
            bigquery.SchemaField(name, "STRING")  # define STRING como padrão
            for name in sample_row.keys()
        ]

        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Tabela {table_id} criada com sucesso!")

# Função para carregar dados no BigQuery
def load_to_bigquery(**kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids='extract_mysql')
    if not rows:
        print("Nenhum dado para carregar no BigQuery.")
        return

    client = bigquery.Client()

    # Cria a tabela, se necessário
    create_bq_table_if_not_exists(client, BQ_DATASET, BQ_TABLE, rows)

    # Insere os dados
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Erro ao inserir no BigQuery: {errors}")
    else:
        print("Dados carregados no BigQuery com sucesso.")

# Função para extrair dados do BigQuery
def extract_from_bigquery(**kwargs):
    client = bigquery.Client()
    query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
    query_job = client.query(query)
    rows = [dict(row) for row in query_job]
    return rows

# Função para carregar no MySQL
def load_to_mysql(**kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids='extract_bigquery')
    if not rows:
        print("Nenhum dado para inserir no MySQL.")
        return

    conn = mysql.connector.connect(**MYSQL_CONN)
    cursor = conn.cursor()
    for row in rows:
        # Ajuste as colunas conforme necessário!
        cursor.execute(
            "INSERT INTO tabela_destino (col1, col2) VALUES (%s, %s)",
            (row['col1'], row['col2'])
        )
    conn.commit()
    cursor.close()
    conn.close()
    print("Dados carregados no MySQL com sucesso.")

with DAG(
    dag_id='mysql_bigquery_bidirectional_auto_create',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['mysql', 'bigquery', 'etl', 'auto_create']
) as dag:

    extract_mysql = PythonOperator(
        task_id='extract_mysql',
        python_callable=extract_from_mysql
    )

    load_bigquery = PythonOperator(
        task_id='load_bigquery',
        python_callable=load_to_bigquery
    )

    extract_bigquery = PythonOperator(
        task_id='extract_bigquery',
        python_callable=extract_from_bigquery
    )

    load_mysql = PythonOperator(
        task_id='load_mysql',
        python_callable=load_to_mysql
    )

    extract_mysql >> load_bigquery >> extract_bigquery >> load_mysql
