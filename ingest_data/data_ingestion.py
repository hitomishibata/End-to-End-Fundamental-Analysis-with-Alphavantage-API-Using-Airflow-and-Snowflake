import requests
import boto3
import json
import csv
from os import environ as env
from dotenv import find_dotenv, load_dotenv

#firstly get the env info
#secondly create function to read csv files
#thirdly create a function to get json data from API
#forthly load them into s3 bucket

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

SYMBOLS = []
BUCKET_NAME = 

def get_csv_from_api(topic: str):
    csv_url = f"https://www.alphavantage.co/query?function={topic}&apikey={env.get('API_KEY')}"
    with requests.Session() as s:
        download = s.get(csv_url)
        download.

def get_json_from_api(topic: str, symbol: str):    
    if symbol is None:
        url = f"https://www.alphavantage.co/query?function={topic}&apikey={env.get('API_KEY')}"
    else:
        url = f"https://www.alphavantage.co/query?function={topic}&symbol={symbol}&apikey={env.get('API_KEY')}"
        
    response = requests.get(url)
    #should I convert the json data into python object?
    json_object = response.json()
    json.dumps(json_object, indent=4)




