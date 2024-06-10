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
from airflow.contrib.hooks.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()
api_key = os.getenv("API_KEY")
#Set the cloud object storage path
base = ObjectStoragePath("s3://container name/", conn_id="idname")
base_url = "https://finnhub.io/api/v1"

default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
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

def ingest_raw_stock_market_json_file():

    @task()
    def ingest_raw_json_file(url, file_name):
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        json_object = json.dumps(json_data, indent=4)
        path = base/f"raw_{file_name}.json"
        with path.open("w") as outfile:
            outfile.write(json_object)
        return path
        
    @task()
    def ready():
        pass

    ingest_raw_json_file(f"{base_url}/stock/metric?symbol=AAPL&metric=all&token={api_key}", "basic_financials")
    ingest_raw_json_file(f"{base_url}/stock/profile2&token={api_key}", "company_profile2")
    ingest_raw_json_file(f"{base_url}/stock/financials-reported&token={api_key}", "financials_reported")
    ingest_raw_json_file(f"{base_url}/stock/earnings?symbol=AAPL&token={api_key}", "stock_earnings_url")
    ready()

ingest_raw_stock_market_json_file()


    

