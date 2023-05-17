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

def updateDaily_Timeseries(json_new, collection):
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
                return
            else:
                new_document['Data'] = new_document['Data'] + i['Data']
                collection.find_one_and_replace(filter={'timestamp': i['timestamp']},
                                                replacement=new_document)


# Create a mongodb client, use default local host
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
except Exception:
    print("Error: " + Exception)


# Ticker list from https://www.nasdaq.com/market-activity/stocks/screener. Filter for NOT nano-cap
# Tickers directory and file name
tick_dir = '/Users/foscoantognini/Documents/USEquityData/Tickers/'
tick_name =  'nasdaq_screener_1684349924023.csv'

nasdaq_data = pd.read_csv(tick_dir + tick_name)
nasdaq_data.drop(columns=['Last Sale', 'Net Change', '% Change', 'Volume'],
                 inplace=True)

# select the collection Nasdaq
collection_NASDAQ_Screener = client.Financial_Data.NASDAQ_Screener

# collection_NASDAQ_Screener.insert_many(nasdaq_data.to_dict(orient='records'))
nasdaq_symbols = collection_NASDAQ_Screener.distinct('Symbol')


# select the collection Daily_Times_Series
collection_Daily_Time_Series = client.Financial_Data.Daily_Time_Series

# print unique symbols in the database
db_symbols = collection_Daily_Time_Series.distinct('Data.Symbol')

print("There are " +
      str(len(db_symbols)) +
      " symbols in the Daily_Timeseries database: " +
      str(db_symbols))

# Start time series
start_date = '2023-04-01'
end_date = '2023-04-28'


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


updateDaily_Timeseries(json1, collection_Daily_Time_Series)
updateDaily_Timeseries(json_new_date, collection_Daily_Time_Series)
updateDaily_Timeseries(json_new_tck, collection_Daily_Time_Series)










