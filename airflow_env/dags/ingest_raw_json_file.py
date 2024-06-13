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

BASE_URL = "https://finnhub.io/api/v1"
BUCKET_NAME = "stockmarket-airflow-dev-bucket"
# create tuples for endpoint and file names
ENDPOINTS_AND_FILE_NAMES = [
    ("stock/metric?symbol=AAPL&metric=all", "basic_financials_AAPL"),
    ("stock/profile2?symbol=AAPL", "company_profile2_AAPL"),
    ("stock/insider-transactions?symbol=AAPL", "insider_transactions_AAPL"),
    ("stock/insider-sentiment?symbol=AAPL&from=2015-01-01&to=2024-03-01", "insider_sentiment_AAPL_2015-01-01_to_2024-03-01"),
    ("stock/financials-reported?cik=320193&freq=quarterly", "financials_reported_quarterly")
]

default_args = {
    'depends_on_past': False,
    'owner': 'hitomi',
    'start_date': datetime(2024, 5, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'@daily',
    'catchup': False
}

@dag(default_args=default_args, schedule='@daily', tags=['first_try'])
def ingest_raw_stock_market_json_file05():
    @task()
    def ingest_raw_json_file_to_s3(endpoint, file_name) -> None:
        # get the json object from API
        url = f"{BASE_URL}/{endpoint}&token={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        # perse JSON data
        json_data = response.json()
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

    for endpoint, file_name in ENDPOINTS_AND_FILE_NAMES:
        ingest_task = ingest_raw_json_file_to_s3(endpoint, file_name)
        ingest_tasks.append(ingest_task)
    
    ready_task = ready()
    chain(*ingest_tasks, ready_task)

ingest_raw_stock_market_json_file05()


    

