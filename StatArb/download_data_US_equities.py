# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap

import pandas as pd
import yfinance as yf
import random
import time
import datetime
import plotly.express as px

# directory where the files will be written
data_dir = '/Users/foscoantognini/Documents/USEquityData/Price_Volume_Dailies/' # home
# data_dir = 'C:\\Users\\Fosco\\Desktop\\Fosco\\USEquityData\\' # office

# load tickers
tick_dir = '/Users/foscoantognini/Documents/USEquityData/Tickers/nasdaq_screener_02052023.csv' # home
# tick_dir = 'C:/Users/Fosco/Desktop/Fosco/Tickers/' # office
Nasdaq_df = pd.read_csv(tick_dir, sep=',')
all_tickers = list(Nasdaq_df.Symbol)

#
d0 = datetime.date(2023, 4, 1)
dF = datetime.date(2023, 4, 28)

# read files with downloaded data and store it in list
already_downloaded_tickers = []
old_df = pd.DataFrame()

for i in ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']:
    locals()[f'old_df_{i}'] = pd.DataFrame()

while len(already_downloaded_tickers) < len(all_tickers):

    # print progress percentage
    print('[*********************'
          + str(round(len(already_downloaded_tickers)/len(all_tickers)*100,3))
          + "%" +
          '**********************]  ' +
          str(len(already_downloaded_tickers)) +
          ' of '
          + str(len(all_tickers)) +
          ' completed')

    # remaining tickers
    remaining_tickers = list(set(all_tickers) - set(already_downloaded_tickers))

    # select tickers to be downloaded
    random_integer = random.randint(2, 7)
    tickers_to_download = random.sample(remaining_tickers, random_integer)

    # download data from yahoo finance
    df = yf.download(tickers_to_download, group_by='Ticker', start=start_date, end=end_date)

    variables = list(df.stack(level=0).columns)

    # write one csv file with the data for each variable
    for i in variables:

        # data with current variable
        locals()[f'new_df_{i}'] = df.stack(level=0)[i].unstack(level=1)

        # Merge the two dataframes on their common column
        locals()[f'old_df_{i}'] = pd.merge(locals()[f'old_df_{i}'], locals()[f'new_df_{i}'], how='outer', left_index=True, right_index=True)

    # write tickers already downloaded
    already_downloaded_tickers = already_downloaded_tickers + tickers_to_download

    # pause program for a random time
    time.sleep(random.randint(15, 30))

for i in ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']:
    locals()[f'old_df_{i}'].to_csv(data_dir + str(i) +'.csv', index=True)



adjclose = pd.read_csv(data_dir + 'Adj Close.csv')







