from datetime import datetime
import os
import requests
import json
import requests.exceptions
import logging


url = 'https://robot-dreams-de-api.herokuapp.com/auth'
headers = {'content-type': 'application/json'}
data = {'username': 'rd_dreams', 'password': 'djT6LasE'}
r = requests.post(url, headers=headers, data=json.dumps(data))
token = r.json()['access_token']

url = 'https://robot-dreams-de-api.herokuapp.com/out_of_stock'
headers = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}
data = {"date": '2022-11-20'}

r = requests.get(url, headers=headers, data=json.dumps(data))
data = r.json()




print(r)
print(data)
