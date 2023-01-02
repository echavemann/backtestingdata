import yfinance as yf
from datetime import datetime, timedelta, date
from tqdm import tqdm
import pandas as pd
import time
import pause

start = '11/12/21'
end = '11/12/22'

def getSP500Data(startDate, endDate, tickerList = None):

  reqCounter = 0
  actualStart = datetime.strptime(startDate, '%m/%d/%y')

  if tickerList == 'SP500':
    URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    listOfSymbols = pd.read_html(URL)[0]["Symbol"].tolist()
  elif tickerList == 'NDX':
    URL = 'https://en.wikipedia.org/wiki/Nasdaq-100'
    pd.read_html(URL)[4]['Ticker'].tolist()
  else:
    listOfSymbols = tickerList

  if tickerList == None:
    return
  
  end = datetime.strptime(endDate, '%m/%d/%y')

  listOfData = {i: pd.DataFrame() for i in listOfSymbols}
  
  if end - timedelta(days = 6) < actualStart:
      for i in tqdm(listOfSymbols):
        listOfData[i] = pd.DataFrame(yf.Ticker(i).history(start = actualStart, end = end, interval="1m", actions = False))
        time.sleep(1)
  else:
    start = end - timedelta(days = 6)

    while start > actualStart:
      for i in tqdm(listOfSymbols):
        listOfData[i] = pd.DataFrame(yf.Ticker(i).history(start = start, end = end, interval="1m", actions = False)).append(listOfData[i])
        
        reqCounter += 1
        if reqCounter >= 100:
          nextHr = datetime.now().replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
          print(f"\n Pausing until the {nextHr} because of rate limits \n")
          pause.until(nextHr)
          reqCounter = 0
        else:
          time.sleep(1)

      end = end - timedelta(days = 6)
      start = start - timedelta(days = 6)

  return listOfData

res = getSP500Data(start, end, tickerList = None)

if res:
  for i in res:
    if not res[i].empty:
      res[i].to_csv(f'{i}.csv', index=False)
    
print('Run Complete.')