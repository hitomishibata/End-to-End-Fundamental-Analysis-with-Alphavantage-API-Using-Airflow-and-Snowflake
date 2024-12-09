import requests
import boto3
from botocore.exceptions import ClientError
import logging
import json
from os import environ as env
from dotenv import find_dotenv, load_dotenv

#store credential inforamtion about aws
#firstly get the env info
#secondly create a function to read csv files
#thirdly create a function to get json data from API
#load them into s3 bucket



ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)
# maximum 25 requests per day with free account
SYMBOLS = ['AAPL', 'MSFT', 'NVDA', 'XOM', 'NEE', 'PG', 'AMZN', 'JPM', 'BRK.B', 'TSLA', 'PLUG']
TOPICS = ["OVERVIEW", "ETF_PROFILE", "DIVIDENDS", "INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW", "EARNINGS"]

BUCKET_NAME = "alphavantage-stock-market-eu-central-1-dev-s3"

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

for symbol in SYMBOLS:
    for topic in TOPICS:
        ingest_json_from_api_to_s3(topic, symbol)





