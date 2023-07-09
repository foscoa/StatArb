from datetime import datetime
import pandas as pd
import numpy as np
import pymongo

def queryTimeSeriesEquity(symbols, start, end, param, collection):
    # Convert to ISO 8601 format
    iso_start = datetime.strptime(start, '%Y-%m-%d').isoformat()
    iso_end = datetime.strptime(end, '%Y-%m-%d').isoformat()

    # Define the aggregation pipeline
    pipeline = [
        {'$match': {'Data.Symbol': {'$in': symbols},
                    "timestamp": {"$gte": iso_start,
                                  "$lt": iso_end}
                    }
         },
        {'$unwind': '$Data'},
        {'$match': {'Data.Symbol': {'$in': symbols}}},
        {'$group': {'_id': '$timestamp', 'Data': {'$push': '$Data'}}},
        {'$project': {'_id': 0, 'timestamp': '$_id', 'Data.Symbol': 1, 'Data.' + param: 1}}
    ]

    json_data = list(collection.aggregate(pipeline))

    # Initialize empty lists to store the parsed data
    stock_data = {}
    df = pd.DataFrame()

    # Iterate over the JSON data and extract the required values
    for item in json_data:
        timestamp = pd.Timestamp(item['timestamp']).to_datetime64()
        for data in item['Data']:
            symbol = data['Symbol']
            adj_close = data['Adj Close']
            if symbol not in stock_data:
                stock_data[symbol] = []
            stock_data[symbol].append([adj_close, timestamp])

    # Create dataframe and merge
    for j in stock_data.keys():
        df_i = pd.DataFrame(stock_data[j], columns=[j, 'Date'])
        df_i.set_index('Date', inplace=True)
        # Sort the DataFrame by the index (timestamps)
        df_i.sort_index(inplace=True)

        # Convert the DataFrame index to a pandas datetime index
        df_i.index = pd.to_datetime(df_i.index)

        df = pd.merge(df, df_i, left_index=True, right_index=True, how='outer')


    # Display the resulting time series DataFrame
    return df

def queryTimeSeriesRates(start, end, collection):
    # Convert to ISO 8601 format
    iso_start = datetime.strptime(start, '%Y-%m-%d').isoformat()
    iso_end = datetime.strptime(end, '%Y-%m-%d').isoformat()

    # Define the aggregation pipeline
    pipeline = [
        {'$match': {"timestamp": {"$gte": iso_start,
                                  "$lt": iso_end}
                    }
         },
        {'$unwind': '$Data'},
        {'$group': {'_id': '$timestamp', 'Data': {'$push': '$Data'}}},
        {'$project': {'_id': 0, 'timestamp': '$_id', 'Data': 1}}
    ]

    json_data = list(collection.aggregate(pipeline))

    # Initialize empty lists to store the parsed data
    timestamps = []
    rates_data = {'EFFR':[], 'SOFR':[]}

    # Iterate over the JSON data and extract the required values
    for item in json_data:
        timestamp = pd.Timestamp(item['timestamp']).to_datetime64()
        timestamps.append(timestamp)
        for data in item['Data']:
            symbol = data['type']

            if len(item['Data']) == 1:
                rates_data['SOFR'].append(np.nan)

            rates_data[symbol].append(data['percentRate'])

    # Create the pandas DataFrame
    df = pd.DataFrame(rates_data, index=timestamps)

    # Sort the DataFrame by the index (timestamps)
    df.sort_index(inplace=True)

    # Convert the DataFrame index to a pandas datetime index
    df.index = pd.to_datetime(df.index)

    # Display the resulting time series DataFrame
    return df

def queryTimeSeriesFactors(start, end, collection):
    # Convert to ISO 8601 format
    iso_start = datetime.strptime(start, '%Y-%m-%d').isoformat()
    iso_end = datetime.strptime(end, '%Y-%m-%d').isoformat()

    # Define the aggregation pipeline
    pipeline = [
        {'$match': {"timestamp": {"$gte": iso_start,
                                  "$lt": iso_end}
                    }
         },
        {'$project': {'_id': 0, 'timestamp':1, 'Mkt-RF':1, 'SMB':1, 'HML':1, 'RF':1, 'WML':1}}
    ]

    json_data = list(collection.aggregate(pipeline))

    # Initialize empty lists to store the parsed data
    timestamps = []
    factors_data = []

    # Iterate over the JSON data and extract the required values
    for item in json_data:
        timestamp = pd.Timestamp(item['timestamp']).to_datetime64()
        timestamps.append(timestamp)
        factors_data.append(list(item.values())[1:])

    # Create the pandas DataFrame
    df = pd.DataFrame(factors_data, index=timestamps)
    df.columns = list(item.keys())[1:]
    # Sort the DataFrame by the index (timestamps)
    df.sort_index(inplace=True)

    # Convert the DataFrame index to a pandas datetime index
    df.index = pd.to_datetime(df.index)

    # Display the resulting time series DataFrame
    return df

def queryGetTickers(collection, date):
    # gets all the tickers in Daily_Timeseries at a given date

    # Convert to ISO 8601 format
    iso_date = datetime.strptime(date, '%Y-%m-%d').isoformat()

    return list(collection.distinct(key='Data.Symbol', filter={"timestamp": iso_date}))

def test_performance_query():
    try:
        client = pymongo.MongoClient()
    except Exception:
        print("Error: " + Exception)

    collection = client.Financial_Data.Daily_Timeseries
    IN_sample_start_date = '2002-01-15'
    IN_sample_end_date = '2023-06-23'
    date = IN_sample_end_date

    stock_tickers_universe = queryGetTickers(collection, date)

    my_time = ['2023-05-23',  # 1 month
               '2023-02-23',  # 3 months
               '2022-11-23',  # 6 months
               '2022-05-23',  # 1 year
               '2020-05-22',  # 3 year
               '2018-05-22',  # 5 year
               '2013-05-22',  # 10 year
               '2008-05-22',  # 15 years
               '2003-05-22',  # 20 years
               ]

    stocks = [1, 2, 4, 8, 16, 32, 64]

    times = pd.DataFrame(0, index=my_time, columns=stocks)

    for i in stocks:
        for k in my_time:
            start_time = time.time()

            stocks_time_series = queryTimeSeriesEquity(symbols=stock_tickers_universe[0:2],
                                                       start=my_time[-1],
                                                       end=IN_sample_end_date,
                                                       param='Adj Close',
                                                       collection=collection)

            end_time = time.time()
            execution_time = end_time - start_time

            minutes = int(execution_time / 60)  # Integer division to get the number of minutes
            remaining_seconds = int(round(execution_time % 60, 0))  # Modulus operation to get the remaining seconds

            exec_time = str(minutes) + "mim" + str(remaining_seconds) + 'sec'

            print("Execution time for " + str(i) + "stocks and " + k + " : " + exec_time)
