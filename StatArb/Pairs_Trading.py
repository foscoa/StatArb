import numpy as np
import pandas as pd
import yfinance as yf
import plotly.express as px
import pymongo
from StatArb.utils.query_mongoDB_functions import *
from StatArb.utils.routine_functions import *
import time


class PairsTradingStrategy:
    def __init__(self):
        # Initialize collection MongoDB database
        collection_equity = ""
        collection_rates = ""
        collection_indices = ""

        # Initialize any necessary variables or parameters here
        IN_sample = False
        IN_sample_start_date = ""
        IN_sample_end_date = ""


    def signal_generation(self):
        # Perform data analysis and generate trading signals
        # This method should take historical data as input and return trading signals

        try:
            client = pymongo.MongoClient()
        except Exception:
            print("Error: " + Exception)

        collection = client.Financial_Data.Daily_Timeseries
        IN_sample_start_date = '2002-01-15'
        IN_sample_end_date = '2023-06-23'
        date = IN_sample_end_date

        stock_tickers_universe = queryGetTickers(collection, date)

        my_time = ['2023-05-23', # 1 month
                '2023-02-23', # 3 months
                '2022-11-23', # 6 months
                '2022-05-23', # 1 year
                '2020-05-22', # 3 year
                '2018-05-22', # 5 year
                '2013-05-22', # 10 year
                '2008-05-22', # 15 years
                '2003-05-22', # 20 years
                ]

        stocks = [1,2,4,8,16,32,64,128, 256, 512, 1024, 2048, 3000]

        times = pd.DataFrame(0, index=time, columns=stocks)

        start_time = time.time()

        stocks_time_series = queryTimeSeriesEquity(symbols      = stock_tickers_universe[0:stocks[3]],
                                                   start        = my_time[4],
                                                   end          = IN_sample_end_date,
                                                   param        = 'Adj Close',
                                                   collection   = collection)
        end_time = time.time()
        execution_time = end_time - start_time

        print("Execution time:", execution_time, "seconds")


    def execute_trades(self, signals):
        # Execute trades based on the generated trading signals
        # This method should take the trading signals as input and execute trades

    def backtest(self, historical_data):
        # Perform backtesting of the strategy
        # This method should take historical data as input and evaluate the strategy's performance

    def live_trading(self):
        # Implement live trading functionality
        # This method should connect to a trading platform and execute real-time trades

    def run(self):
        # The main method that orchestrates the execution of the trading strategy
        # This method should call other methods in the class based on the desired workflow

if __name__ == "__main__":
    strategy = PairsTradingStrategy()
    strategy.run()





