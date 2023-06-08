import pymongo

# Step 1 - Install and import pymongo
import pymongo
import pandas as pd
import plotly.express as px
from datetime import datetime
import numpy as np

# Step 2 - create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# select the collection
collection = client.Financial_Data.Daily_Timeseries

def generateTimeSeries(symbols, start, end, param):
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
    timestamps = []
    stock_data = {}

    # Iterate over the JSON data and extract the required values
    for item in json_data:
        timestamp = pd.Timestamp(item['timestamp']).to_datetime64()
        timestamps.append(timestamp)
        for data in item['Data']:
            symbol = data['Symbol']
            adj_close = data['Adj Close']
            if symbol not in stock_data:
                stock_data[symbol] = []
            stock_data[symbol].append(adj_close)

    # Create the pandas DataFrame
    df = pd.DataFrame(stock_data, index=timestamps)

    # Sort the DataFrame by the index (timestamps)
    df.sort_index(inplace=True)

    # Convert the DataFrame index to a pandas datetime index
    df.index = pd.to_datetime(df.index)

    # Display the resulting time series DataFrame
    return df

def calculate_log_returns(asset_prices):
    # calculate log returns
    log_returns = np.log(asset_prices).shift(-1) - np.log(asset_prices)
    log_returns = log_returns.drop(log_returns.index[-1])
    log_returns.index = asset_prices.index[1:]

    return log_returns

# Specify the symbols you want to retrieve the time series for
symbols = ['CVX', 'STZ']
start = '2001-01-02'
end = '2005-01-30'
param = 'Adj Close'

price_TS = generateTimeSeries(symbols  = symbols,
                              start    = start,
                              end      = end,
                              param    = param)


# calculate log returns
log_returns = np.log(price_TS).shift(-1) - np.log(price_TS)
log_returns = log_returns.drop(log_returns.index[-1])
log_returns.index = price_TS.index[1:]

# signal, long CVX short STZ
signal = price_TS*0
signal.CVX = signal.CVX - 1
signal.STZ = signal.STZ + 1

PT_log_returns = signal*log_returns
PT_log_returns.fillna(0, inplace=True)
PT_aggr = pd.DataFrame(data = PT_log_returns.sum(axis=1) + 1, columns = ['Portfolio'])
line = PT_aggr.cumprod()


class Backtest:
    def __init__(self,
                 name           = "",
                 description    = "",
                 asset_prices   = np.nan,
                 signal         = np.nan):
        self.name = name
        self.description = description
        self.asset_prices = asset_prices
        self.signal = signal

    def calculate_PT_log_returns(self):

        PT_log_returns = self.signal * calculate_log_returns(self.asset_prices)
        PT_log_returns.fillna(0, inplace=True)
        PT_cum_ret = pd.DataFrame(data=PT_log_returns.sum(axis=1) + 1, columns=['Portfolio'])
        PT_cum_ret = PT_cum_ret.cumprod()

        return PT_cum_ret


# Create an instance of the TradingStrategy class
strategy = Backtest(
    name="Momentum Strategy",
    description="Invest in high-performing assets over a certain period",
    asset_prices=price_TS,
    signal=signal
)

# plot cumulative pnl
fig = px.line(line)
fig.show()
















