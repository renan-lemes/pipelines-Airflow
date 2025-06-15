from dotenv import load_dotenv
import os 
import mysql.connector

load_dotenv(dotenv_path="/opt/airflow/dags/.env")

host =  os.getenv('HOST_MYSQL_DOCKER')
user = os.getenv('USERNAME_MYSQL')
password = os.getenv('PASSWORD_MYSQL')
database = os.getenv('MYSQL_DATABASE')
port = 3308

table_origem_mysql = 'olist_customers_dataset'

MYSQL_CONN = {
    'host':'host.docker.internal',             
    'user': user,
    'password': password,
    'database': database,
    'port': 3308   
}

def extract_from_mysql(MYSQL_CONN,table_origem_mysql):
    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONN)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {table_origem_mysql}")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        raise Exception(f"Erro ao extrair do MySQL: {e}")
    finally:
        if cursor :
            cursor.close()
        if conn:
            conn.close()
    return rows

rows = extract_from_mysql(MYSQL_CONN, table_origem_mysql)

print(rows)