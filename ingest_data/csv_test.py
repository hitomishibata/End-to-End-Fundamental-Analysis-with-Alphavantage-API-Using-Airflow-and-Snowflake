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

ENV_FILE = find_dotenv()
if ENV_FILE:
    load_dotenv(ENV_FILE)

csv_url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={env.get('API_KEY')}"
with requests.Session() as s:
    download = s.get(csv_url)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    for row in my_list:
        csv_data = print(row)