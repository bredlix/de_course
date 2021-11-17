from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import requests
import json


dag = DAG(
    dag_id='api_dag',
    description='Extract data from r_d API into shared folder',
    schedule_interval='@daily',
    start_date=datetime(2021, 10, 19, 12, 00)
)


def call(**kwargs):

    dat = kwargs.get('ds')

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

    os.makedirs(os.path.join(os.getcwd(), 'shared_folder', dat), exist_ok=True)

    with open(os.path.join(os.getcwd(), 'shared_folder', dat, dat + '.json'), 'w') as json_file:
        json.dump(data, json_file)

    print(f'Extracted data for {dat}')


t1 = PythonOperator(
    task_id='step_1',
    dag=dag,
    python_callable=call,
    provide_context=True
)