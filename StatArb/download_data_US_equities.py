# List from https://www.nasdaq.com/market-activity/stocks/screener. NOT nano-cap

import pandas as pd
import yfinance as yf
import plotly.express as px

Tickers = pd.read_csv('C:\\Users\\Fosco\\Desktop\\Tickers\\nasdaq_screener_1682963117826.csv',
                      sep=',')

my_ticker = Tickers.Symbol[0]

stock_a = yf.download(my_ticker, start='2005-01-01', end='2023-04-28')

tickerStrings = ['AAPL', 'MSFT']
df1 = yf.download(tickerStrings, group_by='Ticker', period='2d')
df2 = df1.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)