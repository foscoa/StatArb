import numpy as np
import pandas as pd
import yfinance as yf
import plotly.express as px
from StatArb.utils.query_mongoDB_functions import *
from StatArb.utils.routine_functions import *


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
        stock_tickers_universe = queryGetTickers(collection, date)

        stocks_time_series = queryTimeSeriesEquity(symbols      = stock_tickers_universe,
                                                   start        = IN_sample_start_date,
                                                   end          = IN_sample_end_date,
                                                   param        = 'Adj Close',
                                                   collection   = collection)

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





