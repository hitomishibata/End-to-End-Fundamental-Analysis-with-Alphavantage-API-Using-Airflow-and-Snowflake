import logging
import os
import requests
import json

from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from itertools import chain

# get the API key in the .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")

def get_json_data(endpoint):
    response = requests.get(endpoint)
    response.raise_for_status()
    dict_obj = response.json()
    return dict_obj

symbol_dict_obj = get_json_data(f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={API_KEY}")
symbols = []
for item in symbol_dict_obj:
    symbols.append(item['symbol'])

ENDPOINTS_AND_FILE_NAMES = [
    ("stock/financials-reported?cik=320193&freq=quarterly", "financials_reported_quarterly")
]
for symbol in symbols:
    ENDPOINTS_AND_FILE_NAMES.append((f"stock/metric?symbol={symbol}&metric=perShare", f"basic_financials_{symbol}"))
    ENDPOINTS_AND_FILE_NAMES.append((f"stock/insider-transactions?symbol={symbol}", f"insider_transactions_{symbol}"))

BASE_URL = "https://finnhub.io/api/v1"
BUCKET_NAME = "stockmarket-airflow-dev-bucket"

default_args = {
    'depends_on_past': False,
    'owner': 'hitomi',
    'start_date': datetime(2024, 7, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'@daily',
    'catchup': False
}

@dag(default_args=default_args, schedule='@daily', tags=['stock_market'])
def ingest_raw_stock_market_json_file09():
    @task()
    def ingest_raw_json_file_to_s3(endpoint, file_name) -> None:
        # get the json object from API
        url = f"{BASE_URL}/{endpoint}&token={API_KEY}"
        json_data = get_json_data(url)
        json_object = json.dumps(json_data, indent=4)
        # load the data into s3 bucket
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_hook.get_conn().put_object(
            Bucket=BUCKET_NAME,
            Key=f"raw_data/{file_name}.json",
            Body=json_object,
            ContentType='application/json'
        )

        logging.info(f"Uploaded {file_name}.json to s3 bucket {BUCKET_NAME}")
        
    # add the ready task to make it clear which task is the last task in the dag    
    @task()
    def ready() -> None:
        pass

    ingest_tasks = []

# too much data to be procesed so I need to break it down to multiple parts
    for endpoint, file_name in ENDPOINTS_AND_FILE_NAMES[0:3]:
        ingest_task = ingest_raw_json_file_to_s3(endpoint, file_name)
        ingest_tasks.append(ingest_task)
    
    ready_task = ready()
    chain(*ingest_tasks, ready_task)

ingest_raw_stock_market_json_file09()
    

