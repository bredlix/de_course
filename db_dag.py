from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import os


dag = DAG(
    dag_id='db_dag',
    description='Export Postgres DB dump',
    schedule_interval='@daily',
    start_date=datetime(2021, 10, 19, 12, 00)
)


def call(**kwargs):

    pg_creds = {
        'host': '192.168.0.107'
        , 'port': '5432'
        , 'database': 'dshop'
        , 'user': 'pguser'
        , 'password': 'secret'
    }

    dat = kwargs.get('ds')

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        cursor.execute("select table_name from information_schema.tables where table_schema='public'")
        result = cursor.fetchall()
        tables = [row[0] for row in result]

    os.makedirs(os.path.join(os.getcwd(), 'shared_folder', 'db_dumps', dat), exist_ok=True)

    for i in tables:
        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()
            with open(file=os.path.join(os.getcwd(), 'shared_folder', 'db_dumps', dat, f'{i}.csv'),
                      mode='w', encoding='utf-8') as csv_file:
                cursor.copy_expert(f'COPY public.{i} TO STDOUT WITH HEADER CSV', csv_file)

    print(f'Exported dshop tables dump for {dat}')


t1 = PythonOperator(
    task_id='step_1',
    dag=dag,
    python_callable=call,
    provide_context=True
)
