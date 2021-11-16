from datetime import datetime
from requests.exceptions import HTTPError
import os, logging, requests,json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from pyspark.sql import SparkSession

from hdfs import InsecureClient


def dhop_oos_to_bronze(**kwargs):

    etl_date = kwargs.get('ds')

    try:

        url_aut = 'https://robot-dreams-de-api.herokuapp.com/auth'
        header_aut = {'content-type': 'application/json'}
        data_aut = {'username': 'rd_dreams', 'password': 'djT6LasE'}
        r = requests.post(url_aut, headers=header_aut, data=json.dumps(data_aut))
        token = r.json()['access_token']

        url = 'https://robot-dreams-de-api.herokuapp.com/out_of_stock'
        header = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}
        data = {"date": etl_date}

        response = requests.get(url, headers=header, data=json.dumps(data))
        data = response.json()
        response.raise_for_status()

        client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

        logging.info(f"Getting Out of stock product list for {etl_date}")

        with client.write(os.path.join('/', 'datalake', 'bronze', 'out_of_stock_api', f'{etl_date}.json'),
                          encoding='utf-8') as json_file:
            json.dump(data, json_file)

        logging.info(f"Loaded Out of stock product list for {etl_date} into Bronze")

    except HTTPError as e:
        if e.response.status_code == 404:
            logging.info(f"No data for {etl_date}")
        else:
            logging.info(" Х Т Т П ошибка")


def dhop_oos_to_silver(**kwargs):

    etl_date = kwargs.get('ds')

    spark = SparkSession.builder\
        .master('local')\
        .appName('dshop_oos_to_silver')\
        .getOrCreate()

    logging.info(f'Writing Out of stock product list for {etl_date} into Silver')

    df = spark.read.json(os.path.join('/', 'datalake', 'bronze', 'out_of_stock_api', f'{etl_date}.json'))
    df.write.parquet(
        os.path.join('/', 'datalake', 'silver', 'out_of_stock'),
        mode='append')

    logging.info(f"{df.count()} rows written")
    logging.info("Successfully loaded")

dag = DAG(
    dag_id='api_pipeline',
    description='Export api dumps to HDFS',
    schedule_interval='@daily',
    start_date=datetime(2021, 11, 8, 12, 00)
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

start >> api_data_to_bronze >> end >> api_data_to_silver >> end2
