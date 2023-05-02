# List from https://www.nasdaq.com/market-activity/stocks/screener. NOT nano-cap

import pandas as pd
import yfinance as yf

Tickers = pd.read_csv('C:\\Users\\Fosco\\Desktop\\Tickers\\nasdaq_screener_1682963117826.csv',
                      sep=',')

msft = yf.Ticker("MSFT")