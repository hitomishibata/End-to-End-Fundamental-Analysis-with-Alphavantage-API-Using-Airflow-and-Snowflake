import requests
import pandas as pd
import csv
import re
import boto3
from botocore.exceptions import ClientError
import logging
import json
from os import environ as env
from dotenv import find_dotenv, load_dotenv

#firstly get the env info
#secondly create a function to read csv files
#thirdly create a function to get json data from API
#forthly load them into s3 bucket
#store credential inforamtion about aws


ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

SYMBOLS = ['AAPL', 'AXP', 'BA', 'CAT', 'CSCO', 'CVX', 'DIS', 'DOW', 'GS', 'HD']
TOPICS = ["OVERVIEW", "ETF_PROFILE", "DIVIDENDS", "INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW", "EARNINGS"]
#make a tuple from the two lists (symbols and topics)
#a symbol has to be coupled with all elements in topics.[(topic1, symbol1), (topic2, symbol1)...]
BUCKET_NAME = "alphavantage-stock-market-ap-northeast-3-dev-s3"

def ingest_json_from_api_to_s3(topic, symbol) -> None:   
    url = f"https://www.alphavantage.co/query?function={topic}&symbol={symbol}&apikey={env.get('API_KEY')}"
    response = requests.get(url)
    dict_object = response.json()
    data = bytes(json.dumps(dict_object).encode('UTF-8'))
    s3_client = boto3.client(
        service_name='s3',
        region_name=env.get("region"),
        aws_access_key_id=env.get("aws_access_key_id"),
        aws_secret_access_key=env.get("aws_secret_access_key")
    )
    try:
        response = s3_client.put_object(Body=data, Bucket=BUCKET_NAME, Key=f"raw/{topic}_{symbol}.json")
    except ClientError as e:
        logging.error(e)
        return False
    return True
#how can i pass the object name?

for symbol in SYMBOLS:
    for topic in TOPICS:
        ingest_json_from_api_to_s3(topic, symbol)





