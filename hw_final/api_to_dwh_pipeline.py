from datetime import datetime
import os, logging, requests, json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StructType

from hdfs import InsecureClient


def dhop_oos_to_bronze(**kwargs):

    etl_date = kwargs.get('ds')

    api_conn = BaseHook.get_connection('api_oos')

    header_aut = {'content-type': 'application/json'}
    data_aut = {'username': api_conn.login, 'password': api_conn.password}
    r = requests.post(api_conn.host+'/auth', headers=header_aut, data=json.dumps(data_aut))
    token = r.json()['access_token']

    header = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}
    data = {"date": etl_date}

    response = requests.get(api_conn.host+'/out_of_stock', headers=header, data=json.dumps(data))
    data = response.json()

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

    logging.info(f"Getting Out of stock product list for {etl_date}")

    with client.write(os.path.join('/', 'datalake', 'bronze', 'out_of_stock_api', f'{etl_date}.json'),
                      encoding='utf-8') as json_file:
        json.dump(data, json_file)

    logging.info(f"Loaded Out of stock product list for {etl_date} into Bronze")


def dhop_oos_to_silver(**kwargs):

    etl_date = kwargs.get('ds')

    spark = SparkSession.builder\
        .master('local')\
        .appName('dshop_oos_to_silver')\
        .getOrCreate()

    logging.info(f'Writing Out of stock product list for {etl_date} into Silver')

    schema = StructType()\
        .add('date', DateType(), False)\
        .add('product_id', IntegerType(), False)

    df = spark.read.schema(schema).json(os.path.join('/', 'datalake', 'bronze', 'out_of_stock_api', f'{etl_date}.json'))
    df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'out_of_stock'),
        mode='append')

    logging.info(f"{df.count()} rows written")
    logging.info("Successfully loaded")


def dhop_oos_to_dwh(**kwargs):

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

    df = spark.read.parquet(os.path.join('/', 'datalake', 'bronze', 'out_of_stock_api', f'{etl_date}'))

    logging.info(f'Writing Out of stock product list for {etl_date} into DWH')

    df.write.jdbc(gp_url, table='out_of_stock', properties=gp_creds, mode='append')

    logging.info(f"{df.count()} rows written")
    logging.info("Successfully loaded")


dag = DAG(
    dag_id='api_dwh_pipeline',
    description='API Processing Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2021, 11, 17, 12, 00)
)

start = DummyOperator(dag=dag, task_id='transfer_start')
end = DummyOperator(dag=dag, task_id='transfer_to_bronze_end')
end2 = DummyOperator(dag=dag, task_id='transfer_to_silver_end')

api_data_to_bronze = PythonOperator(
    task_id='api_data_to_bronze',
    dag=dag,
    python_callable=dhop_oos_to_bronze,
    provide_context=True)

api_data_to_silver = PythonOperator(
    task_id='api_data_to_silver',
    dag=dag,
    python_callable=dhop_oos_to_silver,
    provide_context=True)

api_data_to_dwh = PythonOperator(
    task_id='dhop_oos_to_dwh',
    dag=dag,
    python_callable=dhop_oos_to_dwh,
    provide_context=True)

start >> api_data_to_bronze >> end >> api_data_to_silver >> end2 >> api_data_to_dwh
