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
collection_equity = client.Financial_Data.Daily_Timeseries
collection_rates = client.Financial_Data.Risk_Free

def generateTimeSeriesEquity(symbols, start, end, param, collection):
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

def generateTimeSeriesRates(start, end, collection):
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


def calculate_log_returns(asset_prices):
    # calculate log returns
    log_returns = np.log(asset_prices).shift(-1) - np.log(asset_prices)
    log_returns = log_returns.drop(log_returns.index[-1])
    log_returns.index = asset_prices.index[1:]

    return log_returns

# Specify the symbols you want to retrieve the time series for
symbols = ['AAPL', 'KO']
start = '2015-01-02'
end = '2023-01-30'
param = 'Adj Close'

price_TS = generateTimeSeriesEquity(symbols      = symbols,
                                    start        = start,
                                    end          = end,
                                    param        = param,
                                    collection   = collection_equity)

rates_TS = generateTimeSeriesRates(start         = start,
                                   end           = end,
                                   collection    = collection_rates)

# Merge the two DataFrames based on a common time index
EFFR_TS = pd.DataFrame(pd.merge(price_TS , rates_TS, left_index=True, right_index=True, how='left')['EFFR'])
# Interpolate the missing values
EFFR_TS = EFFR_TS.interpolate()
# Smooth the values using a simple moving average
# window_size = 10
# EFFR_smoothed = EFFR_TS.rolling(window_size).mean()
# EFFR_smoothed['EFFR'][0:(window_size-1)] = EFFR_TS['EFFR'][0:(window_size-1)]
EFFR_TS = EFFR_TS/(100*252)

# calculate log returns
log_returns = np.log(price_TS).shift(-1) - np.log(price_TS)
log_returns = log_returns.drop(log_returns.index[-1])
log_returns.index = price_TS.index[1:]

# signal, long CVX short STZ
signal = price_TS*0
signal[symbols[0]] =- 1
signal[symbols[1]] += 1

PT_log_returns = signal*log_returns
PT_log_returns.fillna(0, inplace=True)
PT_aggr = pd.DataFrame(data=PT_log_returns.sum(axis=1)+1, columns=['Portfolio'])
line = PT_aggr.cumprod()

# drawdown

class backtest:
    def __init__(self,
                 name           = "",
                 description    = "",
                 asset_prices   = np.nan,
                 risk_free      = np.nan,
                 signal         = np.nan):
        self.name = name
        self.description = description
        self.asset_prices = asset_prices
        self.risk_free = risk_free
        self.signal = signal

    def portfolio_log_returns(self):

        PT_log_returns = self.signal * calculate_log_returns(self.asset_prices)
        PT_log_returns.fillna(0, inplace=True)
        PT_log_returns = pd.DataFrame(data=PT_log_returns.sum(axis=1), columns=['Portfolio'])

        return PT_log_returns

    def portfolio_cumulative_log_returns(self):

        PT_cum_ret = (self.portfolio_log_returns()+1).cumprod()

        return PT_cum_ret

    def drawdown(self):

        PT_cum_ret = self.portfolio_cumulative_log_returns()
        DD = (PT_cum_ret - PT_cum_ret.cummax())

        return DD

    def max_DD(self):
        PT_cum_ret = self.portfolio_cumulative_log_returns()

        DD = self.drawdown()
        through = DD.idxmin()
        when_through = pd.to_datetime(through.values[0])
        start_DDs = DD[DD == 0].dropna().index
        when_peak_max_DD = start_DDs[start_DDs < when_through][-1]
        maxDD = 1 - float(PT_cum_ret[PT_cum_ret.index == when_through].values[0]) / float(
            PT_cum_ret[PT_cum_ret.index == when_peak_max_DD].values[0])

        return maxDD

    def calculate_summary_statistics(self):

        summary_stat = {}

        PT_log_returns = self.portfolio_log_returns()
        PT_excess_returns = pd.DataFrame(PT_log_returns['Portfolio'] - EFFR_TS['EFFR'])

        summary_stat["ann. mean"] = float(PT_log_returns.mean().values) * 252
        summary_stat["ann. std"] = float(PT_log_returns.std().values) * np.sqrt(252)
        summary_stat["max DD"] = self.max_DD()
        summary_stat["sharpe ratio"] = float(PT_excess_returns.mean().values)*252/\
                                       (float(PT_log_returns.std().values) * np.sqrt(252))

        return summary_stat



# Create an instance of the TradingStrategy class
strategy = backtest(
    name="Momentum Strategy",
    description="Invest in high-performing assets over a certain period",
    asset_prices=price_TS,
    signal=signal
)

print(strategy.calculate_summary_statistics())


# plot cumulative pnl
fig = px.line(strategy.portfolio_cumulative_log_returns())
fig.show()















