
import pandas as pd
import yfinance as yf
import random
import pymongo
import numpy as np
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

def update_Daily_Timeseries(json_new, collection):
    # updating database
    for i in json_new:
        # check if timeseries already exist
        if not list(collection.find({"timestamp": i['timestamp']})):

            result = collection.insert_one(i)
            print(result.inserted_id)
        else:
            # find current data at specific timestamp
            new_document = collection.find_one({'timestamp': i['timestamp']})

            # check if there are two equal symbols
            set1 = set([j['Symbol'] for j in new_document['Data']])
            set2 = set([k['Symbol'] for k in i['Data']])

            # check length
            if len(set1.intersection(set2)) > 0:
                print("At node " + new_document['_id'] + " there is a duplicate: " +
                      str(set1.intersection(set2)))

            new_document['Data'] = new_document['Data'] + i['Data']
            collection.find_one_and_replace(filter={'timestamp': i['timestamp']},
                                            replacement=new_document)

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
tick_dir = 'C:\\Users\\Fosco\\Desktop\\Fosco\\USEquityData\\' # office
tick_file_names = 'nasdaq_screener_1684394200429.csv'
last_nasdaq_data = pd.read_csv(tick_dir + tick_file_names)

# collection_NDAQ.insert_many(last_nasdaq_data.to_dict(orient='records')) #only to be used in the first time


# select Nasdaq_Data collection
collection_NDAQ = client.Financial_Data.Nasdaq_Data

# Start time series
start_date = '2001-01-01'
end_date = '2023-05-01'

# select Daily_Timesieries collection
collection_DT = client.Financial_Data.Daily_Timeseries

# print unique symbols in the database
DT_symbols = set(collection_DT.distinct('Data.Symbol'))

print("There are " +
      str(len(DT_symbols)) +
      " symbols in the Daily_Timeseries database: " +
      str(DT_symbols))

NDAQ_symbols = set(collection_NDAQ.distinct('Symbol'))

remaining_tickers = list(np.setdiff1d(NDAQ_symbols, DT_symbols, assume_unique=True)[0])

# select tickers to be downloaded
random_integer = random.randint(2, 7)
tickers_to_download = random.sample(remaining_tickers, random_integer)

# download data from yahoo finance
temp_yf = yf.download(tickers_to_download,
                      group_by='Ticker',
                      start=start_date,
                      end=end_date)

json_yf = yf_to_JSON(temp_yf)

update_Daily_Timeseries(json_yf, collection_DT)










