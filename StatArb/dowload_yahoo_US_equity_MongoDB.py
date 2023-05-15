# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap

import pandas as pd
import yfinance as yf
import random
import pymongo
import time
import datetime as dt
import plotly.express as px


def yf_to_JSON(df):
    # create json nested file
    json_dict = {}
    for i in df.index:  # loop through first node timeseries
        timestamp_list = []
        for col in list(df.stack().columns):  # loop through symbols
            if list(df[col].loc[i].dropna()) != []:
                timestamp_list.append({**{"Symbol": col, }, **df[col].loc[i].to_dict()})
        json_dict[i.isoformat()] = timestamp_list

    json_dict = [{"timestamp": key, "Data": json_dict[key]} for key in json_dict.keys()]

    return json_dict
def updateDaily_Timeseries(json_new):
    # updating database
    for i in json_new:
        # check if timeseries already exist
        if not list(collection.find({"timestamp": i['timestamp']})):
            collection.insert_one(i)
        else:
            old_document = collection.find_one({'timestamp': i['timestamp']})
            collection.find_one_and_replace(filter={'timestamp': i['timestamp']},
                                            return_document=old_document['Data'] + json_new_tck[i]['Data'])

# load tickers
tick_dir = '/Users/foscoantognini/Documents/USEquityData/Tickers/nasdaq_screener_02052023.csv' # home
# tick_dir = 'C:/Users/Fosco/Desktop/Fosco/Tickers/' # office
# Nasdaq_df = pd.read_csv(tick_dir, sep=',')
# all_tickers = list(Nasdaq_df.Symbol)

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
start_date = '2023-04-01'
end_date = '2023-04-28'

# read files with downloaded data and store it in list
# already_downloaded_tickers = []

# remaining tickers
# remaining_tickers = list(set(all_tickers) - set(already_downloaded_tickers))

# select tickers to be downloaded
random_integer = random.randint(2, 7)
# tickers_to_download = random.sample(remaining_tickers, random_integer)
tickers_to_download = ["AAPL", "MSFT"]

# download data from yahoo finance
df1 = yf.download(["KO", "AMZN"], group_by='Ticker', start='2023-05-01', end='2023-05-08')
df_new_date = yf.download(["KO", "AMZN"], group_by='Ticker', start='2023-04-01', end='2023-04-08')
df_new_tck = yf.download(["AAPL", "MSFT"], group_by='Ticker', start='2023-05-01', end='2023-05-08')

json1 = yf_to_JSON(df1)
json_new_date = yf_to_JSON(df_new_date)
json_new_tck = yf_to_JSON(df_new_tck)


# insert the documents into the collection
# result = collection.insert_many(json1)
# print the object ids of the inserted documents
# print(result.inserted_ids)

updateDaily_Timeseries(json1)
updateDaily_Timeseries(json_new_date)
updateDaily_Timeseries(json_new_tck)








