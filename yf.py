import yfinance as yf
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
import datetime
import sqlite3
import atexit

#STOCKS = """https://finance.yahoo.com/u/yahoo-finance/watchlists/crypto-volatility-high"""


conn = sqlite3.connect('yahoo_finance.db')


def exit_handler():
    print("stopping....")
    conn.close()
	

atexit.register(exit_handler)

def fetch_hist(symbol):
	data = yf.download(  # or pdr.get_data_yahoo(...
	        # tickers list or string as well
	        tickers = symbol,

	        # use "period" instead of start/end
	        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
	        # (optional, default is '1mo')
	        period = "1mo",

	        # fetch data by interval (including intraday if period < 60 days)
	        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
	        # (optional, default is '1d')
	        interval = "30m",

	        # group by ticker (to access via data['SPY'])
	        # (optional, default is 'column')
	        group_by = 'ticker',

	        # adjust all OHLC automatically
	        # (optional, default is False)
	        #auto_adjust = True,

	        # download pre/post regular market hours data
	        # (optional, default is False)
	        prepost = True,

	        # use threads for mass downloading? (True/False/Integer)
	        # (optional, default is True)
	        threads = True,

	        # proxy URL scheme use use when downloading?
	        # (optional, default is None)
	        proxy = None
	    )

	now = datetime.datetime.now()
	data['Symbol'] = symbol
	data['Trade'] = data.index.strftime("%Y-%m-%d %H:%M:%S")
	data['Created'] =now.strftime("%Y-%m-%d %H:%M:%S")
	print('saving %s', symbol)
	data.to_sql('history', con=conn, index=False, if_exists="append", method="multi")

# browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
# browser.get(STOCKS)
# elem = browser.find_elements_by_css_selector(".data-col0")
# symbols = [value.text for value in elem]
# symbols = symbols[2:]
symbols = ['ETH-USD', 'UNI1-USD', 'ALGO-USD', 'ATOM-USD', 'FTT-USD', 'FIL-USD', 'BSV-USD', 'DASH-USD', 'DCR-USD', 'RVN-USD', 'ZEN-USD', 'XNO-USD', 'SYS-USD', 'RLC-USD', 'META-USD', 'ZNN-USD', 'DMCH-USD', 'OXEN-USD', 'GAME-USD', 'MHC-USD', 'QASH-USD', 'XAS-USD', 'NYZO-USD', 'GLEEC-USD', 'FTX-USD', 'FLASH-USD', 'MTC1-USD', 'CLAM-USD', 'ECC-USD', 'CSC-USD']
for s in symbols:
    fetch_hist(s.strip())

#browser.close()
conn.close()