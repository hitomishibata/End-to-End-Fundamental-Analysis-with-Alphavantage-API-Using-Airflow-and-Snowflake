import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval':'@daily',
    'catchup': False
}

load_dotenv()
api_key = os.getenv("API_KEY")

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

        with open(file_name, "w") as outfile:
            outfile.write(json_object)
        
    @task()
    def ready():
        pass

    raw_stockmarket_file = ingest_raw_json_file(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey={api_key}",
                   'raw_stockmarket.json')
    ready_task = ready()
    raw_stockmarket_file >> ready_task

ingest_raw_stock_market_json_file()


    

