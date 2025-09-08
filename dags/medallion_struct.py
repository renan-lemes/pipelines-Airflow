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

