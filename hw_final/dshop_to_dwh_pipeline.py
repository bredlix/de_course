from datetime import datetime
import os
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook

from pyspark.sql import SparkSession


def dshop_load_bronze(table_name,**kwargs):

    etl_date = kwargs.get('ds')

    pg_conn = BaseHook.get_connection('postgres_dshop')
    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/dshop"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}

    spark = SparkSession.builder\
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.3.1.jar')\
        .master('local')\
        .appName('dshop_to_bronze')\
        .getOrCreate()

    logging.info(f"Writing table {table_name} from {pg_conn.host} to Bronze")

    table_df = spark.read.jdbc(pg_url, table=table_name, properties=pg_properties)
    table_df.write.csv(
        os.path.join('/', 'datalake', 'bronze', 'dshop', etl_date, f'{table_name}.csv'),
        sep=',',
        header=True,
        mode="overwrite")

    logging.info(f"{table_df.count()} rows written")
    logging.info("Successfully loaded")


def dshop_load_silver(table_name,**kwargs):

    etl_date = kwargs.get('ds')

    spark = SparkSession.builder\
        .master('local')\
        .appName('dshop_to_silver')\
        .getOrCreate()

    logging.info(f"Writing table {table_name} to silver")

    table_df = spark.read.option("header", "true").csv(os.path.join('/', 'datalake', 'bronze', 'dshop', etl_date, f'{table_name}.csv'))
    table_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'dshop', table_name),
        mode="overwrite")

    logging.info(f"Successfully loaded {table_name} in parquet")


def dshop_clients_load_silver(**kwargs):

    etl_date = kwargs.get('ds')

    spark = SparkSession.builder\
        .master('local')\
        .appName('dshop_clients_to_silver')\
        .getOrCreate()

    logging.info(f"Writing table clients to silver")

    table_df = spark.read.option("header", "true").csv(os.path.join('/', 'datalake', 'bronze', 'dshop', etl_date, 'clients.csv'))
    table_df = table_df.dropDuplicates()
    table_df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'dshop', 'clients'),
        mode="overwrite")

    logging.info("Successfully loaded clients in parquet")


def dshop_load_dwh(table_name,**kwargs):

    etl_date = kwargs.get('ds')

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.3.1.jar') \
        .master('local')\
        .appName('dshop_oos_to_dwh')\
        .getOrCreate()

    gp_con = BaseHook.get_connection('gp_dwh')
    gp_url = f'jdbc:postgresql://{gp_con.host}:{gp_con.port}/postgres'
    gp_creds = {'user': gp_con.login, 'password': gp_con.password}

    df = spark.read.parquet(os.path.join('/', 'datalake', 'silver', 'dshop', table_name))

    logging.info(f"Writing table {table_name} to DWH")

    df.write.jdbc(gp_url, table=table_name, properties=gp_creds, mode='overwrite')

    logging.info(f"Successfully loaded {table_name} into DWH")
    logging.info(f"{df.count()} rows written")


dag = DAG(
    dag_id='dshop_dwh_pipeline',
    description='Export Postgres DB dumps to DWH',
    schedule_interval='@daily',
    start_date=datetime(2021, 11, 17, 12, 00)
)

start = DummyOperator(dag=dag, task_id='transfer_start')
end = DummyOperator(dag=dag, task_id='transfer_to_bronze_end')
end2 = DummyOperator(dag=dag, task_id='transfer_to_silver_end')
end3 = DummyOperator(dag=dag, task_id='transfer_to_DWH_end')

tables = ['departments', 'clients', 'orders', 'products', 'aisles']
pg_tables_tasks = []

tables_to_silver = ['departments', 'orders', 'products', 'aisles']
pg_tables_to_silver_tasks = []

dshop_to_dwh = []

for table in tables:
    pg_tables_tasks.append(
        PythonOperator(
            task_id=f'export_{table}_to_bronze',
            dag=dag,
            python_callable=dshop_load_bronze,
            provide_context=True,
            op_kwargs={'table_name': f'{table}'}))

for table in tables_to_silver:
    pg_tables_to_silver_tasks.append(
        PythonOperator(
            task_id=f'export_{table}_to_silver',
            dag=dag,
            python_callable=dshop_load_silver,
            provide_context=True,
            op_kwargs={'table_name': f'{table}'}))

pg_tables_to_silver_tasks.append(
    PythonOperator(
        task_id='export_clients_to_silver',
        dag=dag,
        python_callable=dshop_clients_load_silver,
        provide_context=True))

for table in tables:
    dshop_to_dwh.append(
        PythonOperator(
            task_id=f'load_{table}_to_DWH',
            dag=dag,
            python_callable=dshop_load_dwh,
            provide_context=True,
            op_kwargs={'table_name': f'{table}'}))


start >> pg_tables_tasks >> end >> pg_tables_to_silver_tasks >> end2 >> dshop_to_dwh >> end3

