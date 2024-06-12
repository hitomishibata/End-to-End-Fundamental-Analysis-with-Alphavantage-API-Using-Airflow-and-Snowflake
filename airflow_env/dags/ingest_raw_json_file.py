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

default_args = {
    'depends_on_past': False,
    'owner': 'hitomi',
    'start_date': datetime(2024, 5, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'@daily',
    'catchup': False
}

@dag(
    default_args=default_args,
    schedule='@daily',
    tags=['first_try']
)

def ingest_raw_stock_market_json_file04():

    @task()
    def ingest_raw_json_file_to_s3(endpoint, file_name, bucket):
        # get the json object from API
        url = f"{base_url}/{endpoint}&token={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        # perse JSON data
        json_data = response.json()
        json_object = json.dumps(json_data, indent=4)
        # load the data into s3 bucket
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_hook.get_conn().put_object(
            Bucket=bucket,
            Key=f"raw_data/{file_name}.json",
            Body=json_object,
            ContentType='application/json'
        )

        logging.info(f"Uploaded {file_name}.json to s3 bucket {bucket}")
        
       
    # add the ready task to make it clear which task is the last task in the dag    
    @task()
    def ready():
        pass
    
    # get the API key in the .env file
    load_dotenv()
    api_key = os.getenv("API_KEY")

    base_url = "https://finnhub.io/api/v1"
    # create tuples for endpoint and file names
    endpoints_and_files = [
    ("stock/metric?symbol=AAPL&metric=all", "basic_financials_AAPL"),
    ("stock/profile2?symbol=AAPL", "company_profile2_AAPL"),
    ("stock/insider-transactions?symbol=AAPL", "insider_transactions_AAPL"),
    ("stock/insider-sentiment?symbol=AAPL&from=2015-01-01&to=2024-03-01", "insider_sentiment_AAPL_2015-01-01_to_2024-03-01"),
    ("stock/financials-reported?cik=320193&freq=quarterly", "financials_reported_quarterly")
    ]

    for endpoint, file_name in endpoints_and_files:
        ingest_raw_json_file_to_s3(endpoint, file_name, "stockmarket-airflow-dev-bucket") 
    
    ready()

ingest_raw_stock_market_json_file04()


    

