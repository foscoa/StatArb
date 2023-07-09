import numpy as np
import pandas as pd
import yfinance as yf
import plotly.express as px
import pymongo
from StatArb.utils.query_mongoDB_functions import *
from StatArb.utils.routine_functions import *
import time
import statsmodels.api as sm



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

        # conntect to database
        try:
            client = pymongo.MongoClient()
        except Exception:
            print("Error: " + Exception)

        collection_equity = client.Financial_Data.Daily_Timeseries
        collection_FCT = client.Financial_Data.Risk_Factors

        print("Last day in equity time series is " + str(list(collection_equity.distinct("timestamp"))[-1]))
        print("Last day in factors time series is " + str(list(collection_FCT.distinct("timestamp"))[-1]))

        print("First day in equity time series is " + str(list(collection_equity.distinct("timestamp"))[0]))
        print("First day in factors time series is " + str(list(collection_FCT.distinct("timestamp"))[0]))

        IN_sample_start_date = '2015-06-23'
        IN_sample_end_date = '2015-12-23'

        # generate factors time series
        factors_TS = queryTimeSeriesFactors(start=IN_sample_start_date,
                                            end=IN_sample_end_date,
                                            collection=collection_FCT)
        factors_TS /= 100
        factors_TS.drop('RF', axis=1, inplace=True)

        X_factor_exposures = pd.DataFrame(index=['const', 'Mkt-RF', 'SMB', 'HML', 'WML'])


        stock_tickers_universe = queryGetTickers(collection_equity, IN_sample_end_date)

        for symbol in stock_tickers_universe:

            stocks_time_series = queryTimeSeriesEquity(symbols=[symbol],
                                                       start=IN_sample_start_date,
                                                       end=IN_sample_end_date,
                                                       param='Adj Close',
                                                       collection=collection_equity)

            # Calculate stocks returns
            stocks_returns = calculate_log_returns(stocks_time_series)

            # Merge the two time series
            merged_ts = pd.merge(stocks_returns, factors_TS, how='outer', left_index=True, right_index=True)
            merged_ts = pd.DataFrame(merged_ts.loc[merged_ts[symbol].dropna().index])

            # Separate the independent variables (X) and the dependent variable (Y)
            X = pd.DataFrame(merged_ts[['Mkt-RF', 'SMB', 'HML', 'WML']])
            Y = pd.DataFrame(merged_ts[symbol])

            # Add a constant column to X for the intercept term
            X = sm.add_constant(X)

            # Fit the multiple linear regression model
            model = sm.OLS(Y, X).fit()

            # New column data
            new_column = pd.Series(model.params, name=symbol)

            # Concatenate the new column with the original DataFrame
            X_factor_exposures = pd.concat([X_factor_exposures, new_column], axis=1)

            # Print the model summary
            print(str(round(stock_tickers_universe.index(symbol)/len(stock_tickers_universe)*100,2)) + '%')





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





