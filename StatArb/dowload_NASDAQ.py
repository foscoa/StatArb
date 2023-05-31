
import yfinance as yf
import random
import pymongo
import numpy as np
import time


def update_Nasdaq_Data(last_nasdaq_data, collection):
    new_symbols = np.setdiff1d(last_nasdaq_data.Symbol, collection.distinct('Symbol'), assume_unique=True)

    if len(new_symbols) == 1 & np.isnan(new_symbols[0]):
        print('No new symbols to add in Nasdaq_Data')
    else:
        # TODO add new symbols
        print()

# Create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap
# load tickers
# tick_dir = '/Users/foscoantognini/Documents/USEquityData/Tickers/'  # home
# tick_dir = 'C:\\Users\\Fosco\\Desktop\\Fosco\\USEquityData\\' # office
# tick_file_names = 'nasdaq_screener_1684394200429.csv'
# last_nasdaq_data = pd.read_csv(tick_dir + tick_file_names)
# collection_NDAQ.insert_many(last_nasdaq_data.to_dict(orient='records')) #only to be used in the first time


# select Nasdaq_Data collection
collection_NDAQ = client.Financial_Data.NASDAQ_Screener
NDAQ_symbols = list(collection_NDAQ.distinct('Symbol'))[1:]
