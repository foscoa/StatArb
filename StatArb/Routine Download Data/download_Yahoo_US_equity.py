
import yfinance as yf
import random
import pymongo
import time
from datetime import datetime

def yf_to_JSON(df):
    """
    Converts a yahoo finance dataframe in a JSON file.

    :param df: pandas dataframe, which is the output of yf.download function. !! must have at least 2 tickers !!
        Example: yf.download(['^SPX', '^IXIC'], group_by='Ticker', start=start_date, end=end_date)

    :return: json_dict: list of dictonaries, which contains yahoo finance data
        Example: {'timestamp': '2001-01-02T00:00:00',
                  'Data': [{'Symbol': '^IXIC',
                                'Open': 2474.15, 'High': 2474.15, 'Low': 2273.07,
                                'Close': 2291.86, 'Adj Close': 2291.86, 'Volume': 1918930000.0},
                            {'Symbol': '^SPX',
                                'Open': 1320.28, 'High': 1320.28, 'Low': 1276.05, 'Close': 1283.27,
                                 'Adj Close': 1283.27, 'Volume': 1129400000.0}
                            ]
                 }

    """

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
    """

    :param json_new: list of dictonaries, which contains yahoo finance data.
        Example: {'timestamp': '2001-01-02T00:00:00',
                  'Data': [{'Symbol': '^IXIC',
                                'Open': 2474.15, 'High': 2474.15, 'Low': 2273.07,
                                'Close': 2291.86, 'Adj Close': 2291.86, 'Volume': 1918930000.0},
                            {'Symbol': '^SPX',
                                'Open': 1320.28, 'High': 1320.28, 'Low': 1276.05, 'Close': 1283.27,
                                 'Adj Close': 1283.27, 'Volume': 1129400000.0}
                            ]
                 }
    :param collection: mongoDB collection where you want to append the timeseries
    :return: nothing. the function appends new timeseries
    """

    for i in json_new:
        # check if date already exists
        if not list(collection.find({"timestamp": i['timestamp']})):
            collection.insert_one(i)
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
            else:
                new_document['Data'] = new_document['Data'] + i['Data']
                collection.find_one_and_replace(filter={'timestamp': i['timestamp']},
                                                replacement=new_document)

def delete_Daily_Timeseries(start_date,
                            end_date,
                            collection):
    # Convert to ISO 8601 format
    iso_start = datetime.strptime(start_date, '%Y-%m-%d').isoformat()
    iso_end = datetime.strptime(end_date, '%Y-%m-%d').isoformat()

    # Define the query filter
    query = {
        "timestamp": {
            "$gte": iso_start,
            "$lt": iso_end
        }
    }

    # Delete all elements with timestamp within the specified time period
    result = collection.delete_many(query)

    print(f"Deleted {result.deleted_count} documents")

def update_existing_symbol_Daily_Timeseries(start_date,
                                            end_date,
                                            collection):
    """

    :param start_date: included
    :param end_date: excluded
    :param collection:
    :return:
    """

    # find remaining tickers to be downloaded
    remaining_tickers = list(collection.distinct('Data.Symbol'))
    nr_symbol = len(remaining_tickers)

    while len(remaining_tickers) > 0:

        # select tickers to be downloaded
        random_integer = random.randint(15, 30)
        if len(remaining_tickers) < random_integer:
            tickers_to_download = remaining_tickers
        else:
            tickers_to_download = random.sample(remaining_tickers, random_integer)

        # check if ticker data does not have a price in last business date or does not exist
        # for j in tickers_to_download:
        #    if yf.download(j, start=start_date, end=end_date).empty:
        #        print("Last data point for " + j + " is empty.")
        #        defect_tickers.append(j)
         #       tickers_to_download.remove(j)

        # download data from yahoo finance
        temp_yf = yf.download(tickers_to_download,
                              group_by='Ticker',
                              start=start_date,
                              end=end_date)

        if len(tickers_to_download) > 1:
            json_yf = yf_to_JSON(temp_yf)

            update_Daily_Timeseries(json_yf, collection)

        for element in tickers_to_download:
            remaining_tickers.remove(element)


        # print progress percentage
        print('[*********************'
              + str(round(len(remaining_tickers) / nr_symbol * 100, 3))
              + "%" +
              '**********************]  ' +
              str(len(remaining_tickers)) +
              ' of '
              + str(nr_symbol) +
              ' completed\nFollowing tickers have been added: ' + str(tickers_to_download))

        # pause program for a random time
        time.sleep(random.randint(2, 7))


# Create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)


# daily routine to append latest data
collection_Daily_Timeseries = client.Financial_Data.Daily_Timeseries

# Retrieve all days
# days = list(collection_Daily_Timeseries.distinct("timestamp"))

start_time = time.time()

update_existing_symbol_Daily_Timeseries(start_date='2023-05-02',
                                        end_date='2023-05-02',
                                        collection=collection_Daily_Timeseries)

# delete_Daily_Timeseries(start_date='2023-05-01', end_date='2023-05-02', collection=collection_Daily_Timeseries)

end_time = time.time()
execution_time = end_time - start_time

print("Execution time: ", execution_time, " seconds")




# Start time series
start_date = '2023-05-01'
end_date = '2023-05-02'
defect_tickers = []
# dowload_All_YF_Universe()

temp_yf = yf.download(['^SPX', '^IXIC'],
                      group_by='Ticker',
                      start=start_date,
                      end=end_date)

json_new = yf_to_JSON(temp_yf)

# select Nasdaq_Data collection
collection_TS = client.Financial_Data.Indices

update_Daily_Timeseries(json_new, collection_TS)






