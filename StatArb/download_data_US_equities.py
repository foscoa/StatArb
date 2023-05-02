# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap

import pandas as pd
import yfinance as yf
import plotly.express as px

# upload tickers
Tickers = pd.read_csv('/Users/foscoantognini/Documents/USEquityData/Tickers/nasdaq_screener_02052023.csv',
                      sep=',')

# directory where the files will be written
out_data_dir = '/Users/foscoantognini/Documents/USEquityData/Price_Volume_Dailies/'

# my_ticker = Tickers.Symbol[0]

# stock_a = yf.download(my_ticker, start='2005-01-01', end='2023-04-28')

tickerStrings = ['AAPL', 'MSFT']
all_df = yf.download(tickerStrings, group_by='Ticker', start='2005-01-01', end='2023-04-28')
all_df = all_df.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)

# write one csv file with the data for each ticker
for i in tickerStrings:
    single_df = all_df[all_df.Ticker == i]
    single_df.to_csv(out_data_dir +
                     i +
                     '_' +
                     str(single_df.index[0].date())
                     + '_' + str(single_df.index[-1].date()) +
                     '.csv',
                     index=False)

