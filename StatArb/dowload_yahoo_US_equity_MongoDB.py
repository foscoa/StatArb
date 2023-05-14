# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap

import pandas as pd
import yfinance as yf
import random
import pymongo
import time
import datetime as dt
import plotly.express as px


# load tickers
tick_dir = '/Users/foscoantognini/Documents/USEquityData/Tickers/nasdaq_screener_02052023.csv' # home
# tick_dir = 'C:/Users/Fosco/Desktop/Fosco/Tickers/' # office
Nasdaq_df = pd.read_csv(tick_dir, sep=',')
all_tickers = list(Nasdaq_df.Symbol)

# Create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# Step 3 - List database names
print(client.list_database_names())

# select the collection
collection = client.Financial_Data.Daily_Timeseries

# Start time series
start_date = '2005-01-01'
end_date = '2023-04-28'

# read files with downloaded data and store it in list
already_downloaded_tickers = []

# remaining tickers
remaining_tickers = list(set(all_tickers) - set(already_downloaded_tickers))

# select tickers to be downloaded
random_integer = random.randint(2, 7)
tickers_to_download = random.sample(remaining_tickers, random_integer)

# download data from yahoo finance
df = yf.download(tickers_to_download, group_by='Ticker', start=start_date, end=end_date)

# create json nested file
json_dict = {}
for i in df.index: # loop through first node timeseries
    timestamp_list = []
    for col in list(df.stack().columns): # loop through symbols
        if list(df[col].loc[i].dropna())!=[]:
            timestamp_list.append({**{"Symbol": col, }, **df[col].loc[i].to_dict()})
    json_dict[i.isoformat()] = timestamp_list

json_dict = [{"timestamp": key, "Data": json_dict[key]} for key in json_dict.keys()]

# insert the documents into the collection
result = collection.insert_many(json_dict)

# print the object ids of the inserted documents
print(result.inserted_ids)




