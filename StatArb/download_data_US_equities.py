# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap

import pandas as pd
import yfinance as yf
import json
import plotly.express as px

def WriteCsvForEachTicker(df, out_data_dir):

    df = df.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)

    # write one csv file with the data for each ticker
    for i in tickerStrings:
        single_df = df[df.Ticker == i]
        single_df.to_csv(out_data_dir +
                         i +
                         '_' +
                         str(single_df.index[0].date())
                         + '_' + str(single_df.index[-1].date()) +
                         '.csv',
                         index=True)

def WriteCsvForEachVariable(df, out_data_dir):

    variables = list(df.stack(level=0).columns)

    # write one csv file with the data for each variable
    for i in variables:

        single_df = df.stack(level=0)['Adj Close'].unstack(level=1)
        single_df.to_csv(out_data_dir +
                         i +
                         '_' +
                         str(single_df.index[0].date())
                         + '_' + str(single_df.index[-1].date()) +
                         '.csv',
                         index=True)

# upload tickers
Tickers = pd.read_csv('/Users/foscoantognini/Documents/USEquityData/Tickers/nasdaq_screener_02052023.csv',
                      sep=',')

# directory where the files will be written
out_data_dir = '/Users/foscoantognini/Documents/USEquityData/Price_Volume_Dailies/'

# my_ticker = Tickers.Symbol[0]

# stock_a = yf.download(my_ticker, start='2005-01-01', end='2023-04-28')

tickerStrings = ['AAPL', 'MSFT']
df = yf.download(tickerStrings, group_by='Ticker', start='2005-01-01', end='2023-04-28')

# WriteCsvForEachVariable(df,out_data_dir)

# WriteCsvForEachTicker(df,out_data_dir)

# load_csv = pd.read_csv(out_data_dir + 'Adj Close_2005-01-03_2023-04-27.csv', index_col=0)





