from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import csv
import re

URL = "https://ycharts.com/indicators/us_inflation_rate"
REQ = Request(
    url= URL, 
    headers={'User-Agent': 'Mozilla/5.0'}
)
def get_html_text(req):
    page = urlopen(REQ)
    html_bytes = page.read()
    html = html_bytes.decode("utf-8")
    return html

def get_data_in_html_text(html_string):
    soup = BeautifulSoup(html, "html.parser")
    table_list = soup.find_all('table class="table"')
    print(table_list)

html = get_html_text(REQ)
get_data_in_html_text(html)
