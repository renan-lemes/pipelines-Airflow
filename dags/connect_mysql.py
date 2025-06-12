from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import os
import mysql.connector

# Carrega as variáveis de ambiente do .env localizado na pasta /opt/airflow/dags
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

def test_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host='host.docker.internal',
            user=os.getenv("USERNAME_MYSQL"),
            password=os.getenv("PASSWORD_MYSQL"),
            database=os.getenv("MYSQL_DATABASE"),
            port=int(os.getenv("PORT_MYSQL"))
        )

        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        print("✅ Conectado com sucesso! Tabelas encontradas:")
        for table in tables:
            print(f" - {table[0]}")

    except Exception as e:
        print("❌ Erro ao conectar ao MySQL:", e)
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='test_mysql_connection_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['mysql', 'teste'],
) as dag:

    test_conn = PythonOperator(
        task_id='connect_and_show_tables',
        python_callable=test_mysql_connection
    )
