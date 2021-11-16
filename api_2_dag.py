from datetime import datetime
import os
import requests
import json

from hdfs import InsecureClient

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


dag = DAG(
    dag_id='api_2_dag',
    description='Extract data from r_d API into HDFS',
    schedule_interval='@daily',
    start_date=datetime(2021, 11, 9, 12, 00)
)


def api_to_hdfs(**kwargs):

    dat = kwargs.get('ds')
    year = datetime.strptime(dat, '%Y-%m-%d').strftime('%Y')
    month = datetime.strptime(dat, '%Y-%m-%d').strftime('%m')
    day = datetime.strptime(dat, '%Y-%m-%d').strftime('%d')

    url = 'https://robot-dreams-de-api.herokuapp.com/auth'
    headers = {'content-type': 'application/json'}
    data = {'username': 'rd_dreams', 'password': 'djT6LasE'}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']

    url = 'https://robot-dreams-de-api.herokuapp.com/out_of_stock'
    headers = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}
    data = {"date": dat}

    r = requests.get(url, headers=headers, data=json.dumps(data))
    data = r.json()

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

    with client.write(os.path.join('/', 'bronze', 'r_d_API', year, month, day, f'{dat}.json'), encoding='utf-8') as json_file:
        json.dump(data, json_file)

    print(f'Loaded data from r_d API for {dat}')


dum1 = DummyOperator(task_id='start', dag=dag)
dum2 = DummyOperator(task_id='end', dag=dag)

api_call = PythonOperator(
    task_id='get_api_data',
    dag=dag,
    python_callable=api_to_hdfs,
    provide_context=True
)

dum1 >> api_call >> dum2
