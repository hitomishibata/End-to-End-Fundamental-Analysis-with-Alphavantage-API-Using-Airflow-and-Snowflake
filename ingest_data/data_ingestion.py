import requests
import pandas as pd
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

SYMBOLS = ["AAPL",]
TOPICS = ["OVERVIEW", "ETF_PROFILE", "DIVIDENDS", "INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW", "EARNINGS"]
#make a tuple from the two lists (symbols and topics)
#a symbol has to be coupled with all elements in topics.[(topic1, symbol1), (topic2, symbol1)...]
BUCKET_NAME = "alphavantage-stock-market-ap-northeast-3-dev-s3"

def create_pair_of_topic_symbol(symbols: list, topics: list):
    for symbol in SYMBOLS:
        for topic in TOPICS:
            pair = (topic, symbol)
    return pair

def get_csv_from_api(topic: str):
    csv_url = f"https://www.alphavantage.co/query?function={topic}&apikey={env.get('API_KEY')}"
    c = pd.read_csv(csv_url)

def get_json_from_api(topic: str, symbol: str|None) -> None:    
    if symbol is None:
        url = f"https://www.alphavantage.co/query?function={topic}&apikey={env.get('API_KEY')}"
    else:
        url = f"https://www.alphavantage.co/query?function={topic}&symbol={symbol}&apikey={env.get('API_KEY')}"
    response = requests.get(url)
    #should I convert the json data into python object?
    dict_object = response.json()
    json_data = json.dumps(dict_object)
    return json_data

def upload_file(file_name, bucket, object_name):
    s3_client = boto3.client(
        service_name='s3',
        region_name=env.get("region"),
        aws_access_key_id=env.get("aws_access_key_id"),
        aws_secret_access_key=env.get("aws_secret_access_key")
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
#how can i pass the object name?
csv_file = get_csv_from_api("LISTING_STATUS")
csv_file2 = get_csv_from_api("EARNINGS_CALENDAR")
upload_file(csv_file, BUCKET_NAME, "raw/listing_status.csv")
upload_file(csv_file2, BUCKET_NAME, "raw/earnings_calendar.csv")
#get_json_from_api(create_pair_of_topic_symbol(SYMBOLS, TOPICS),BUCKET_NAME, f"raw/{}.json")






