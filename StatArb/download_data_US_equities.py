# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap

import pandas as pd
import yfinance as yf
import json
import plotly.express as px

def WriteCsvForEachVariable(data_dir):

    # download data from yahoo finance
    tickerStrings = ['MCD', 'KO']
    df = yf.download(tickerStrings, group_by='Ticker', start='2005-01-01', end='2023-04-28')

    variables = list(df.stack(level=0).columns)

    # write one csv file with the data for each variable
    for i in variables:

        # data with current variable
        new_df = df.stack(level=0)[i].unstack(level=1)

        # current variable name
        data_string = data_dir + i + '_' + str(new_df.index[0].date()) + '_' + str(new_df.index[-1].date()) + '.csv'

        # load data and reindexing
        merged_df = pd.read_csv(data_string, index_col=0)
        new_df.index = merged_df.index

        # Merge the two dataframes on their common column
        merged_df= pd.merge(merged_df, new_df, on='Date')

        # write merged df
        merged_df.to_csv(data_string, index=True)

# upload tickers

tick_dir = '/Users/foscoantognini/Documents/USEquityData/Tickers/nasdaq_screener_02052023.csv' # home
# tick_dir = 'C:/Users/Fosco/Desktop/Fosco/Tickers/' # office

Tickers = pd.read_csv(tick_dir, sep=',')

# directory where the files will be written
#  data_dir = '/Users/foscoantognini/Documents/USEquityData/Price_Volume_Dailies/'
data_dir = 'C:\\Users\\Fosco\\Desktop\\Fosco\\USEquityData\\' # office

# WriteCsvForEachVariable(df,data_dir)


# load_csv = pd.read_csv(data_dir + 'Adj Close_2005-01-03_2023-04-27.csv', index_col=0)





