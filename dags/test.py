import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from fastavro import writer, parse_schema
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook

#set up logging
logger = logging.getLogger('airflow.task')

#Define the connection parameters 
pg_conn = 'postgres_local_conn'

#Define connection parameter for PostgreSQL connection 
def get_postgressql_connection():
    conn = BaseHook.get_connection(pg_conn)
    user_name = conn.login 
    password = conn.password
    ip = conn.host
    database_name = conn.schema
    connection_string = f"postgresql+psycopg2://{user_name}:{password}@{ip}:5432/{database_name}"
    return create_engine(connection_string) 
def fetch_table_names():
    engine = get_postgressql_connection()
    with engine.connect() as conn:
        query = '''
                SELECT 
                    table_name 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE';
                '''
        table_name = pd.read_sql(query, conn)
        return table_name['table_name'].tolist()

with DAG(
    dag_id='fetch_table',
    default_args={
        'owner': 'airflow'},
    description='Extract data from PostgreSQL to ADLS bronze layer',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    fetch_table_names_task = PythonOperator(
        task_id='fetch_table_names',
        python_callable=fetch_table_names
    )
    fetch_table_names_task