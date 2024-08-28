import yfinance as yf

from bs4 import BeautifulSoup
from urllib.request import urlopen

#get the top stock active companies from yahoo finance
#put them in a list

URL = "https://finance.yahoo.com/markets/stocks/most-active/?guccounter=1&start=0&count=100"
page = urlopen(URL)
html = page.read().decode("utf-8")
soup = BeautifulSoup(html, "html.parser")
print(soup.prettify())
#get the useful element using find_all 
#s = soup.find("", class="")
#content = s.find_all("tag")