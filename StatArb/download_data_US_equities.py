# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap

import pandas as pd
import yfinance as yf
import random
import plotly.express as px

# directory where the files will be written
data_dir = '/Users/foscoantognini/Documents/USEquityData/Price_Volume_Dailies/' # home
# data_dir = 'C:\\Users\\Fosco\\Desktop\\Fosco\\USEquityData\\' # office

# load tickers
tick_dir = '/Users/foscoantognini/Documents/USEquityData/Tickers/nasdaq_screener_02052023.csv' # home
# tick_dir = 'C:/Users/Fosco/Desktop/Fosco/Tickers/' # office
Nasdaq_df = pd.read_csv(tick_dir, sep=',')
all_tickers = list(Nasdaq_df.Symbol)

# read files with downloaded data and store it in list
already_downloaded_tickers = pd.read_csv(data_dir + '/downloaded_tickers.csv')
already_downloaded_tickers = list(already_downloaded_tickers['0'])


while len(already_downloaded_tickers) < len(all_tickers):
    # remaining tickers
    remaining_tickers = list(set(all_tickers) - set(already_downloaded_tickers))

    # select tickers to be downloaded
    random_integer = random.randint(3, 6)
    tickers_to_download = random.sample(remaining_tickers, 5)

    # download data from yahoo finance
    df = yf.download(tickers_to_download, group_by='Ticker', start='2005-01-01', end='2023-04-28')

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
        merged_df = pd.merge(merged_df, new_df, on='Date')

        # write merged df
        merged_df.to_csv(data_string, index=True)


    tickers_to_write = already_downloaded_tickers + tickers_to_download
    pd.DataFrame(tickers_to_write).to_csv(data_dir + '/downloaded_tickers.csv', index=False)










