from datetime import datetime
import os

from hdfs import InsecureClient

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator


def pg_to_hdfs(tab, **kwargs):

    dat = kwargs.get('ds')
    year = datetime.strptime(dat, '%Y-%m-%d').strftime('%Y')
    month = datetime.strptime(dat, '%Y-%m-%d').strftime('%m')
    day = datetime.strptime(dat, '%Y-%m-%d').strftime('%d')

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

    with PostgresHook(postgres_conn_id='postgres_dshop').get_conn() as conn:
        cursor = conn.cursor()
        with client.write(os.path.join('/', 'bronze', 'dshop_dumps', year, month, day, f'{tab}.csv')) as csv_file:
            cursor.copy_expert(f'COPY public.{tab} TO STDOUT WITH HEADER CSV', csv_file)


    print(f'Exported dshop {tab} dump for {dat}')


dag = DAG(
    dag_id='db_2_dag',
    description='Export Postgres DB dumps to HDFS',
    schedule_interval='@daily',
    start_date=datetime(2021, 10, 28, 12, 00)
)

dum1 = DummyOperator(task_id='start', dag=dag)
dum2 = DummyOperator(task_id='end', dag=dag)


tables = ['departments', 'clients', 'orders', 'products', 'aisles']
tables_tasks = []


for table in tables:
    tables_tasks.append(
        PythonOperator(
            task_id=f'export_{table}',
            dag=dag,
            python_callable=pg_to_hdfs,
            provide_context=True,
            op_kwargs={'tab': f'{table}'})
    )

dum1 >> tables_tasks >> dum2
