from bs4 import BeautifulSoup
from urllib.request import urlopen
import requests

#get the top stock active companies from yahoo finance
#put them in a list
headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'}
url = "https://finance.yahoo.com/markets/stocks/most-active/"
response = requests.get(url, headers=headers)
print(response)
soup = BeautifulSoup(response.content, "lxml")
for item in soup.select('.markets-table'):
    print(item.select('[span.class="symbol.yf-ravs5v"]')[0].get_text())
#print(soup.prettify())
#get the useful element using find_all 
#s = soup.find("", class="")
#content = s.find_all("tag")