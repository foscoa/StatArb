import numpy as np
import pandas as pd
import yfinance as yf
import plotly.express as px

# download stock data
equity_tickers = ["AAPL", "MSFT", "MCD"]
equity_data = yf.download(equity_tickers,
                          start='2010-01-01',
                          end='2023-04-28',
                          interval='1mo')

# dataframe with closing price
closing_price = equity_data[equity_data.columns[[i for i, x in enumerate(equity_data.columns) if x[0] == "Adj Close"]]]
closing_price.columns = [j[1] for j in closing_price.columns]

# calculate log returns
log_returns = np.log(closing_price).shift(-1) - np.log(closing_price)
log_returns = log_returns.drop(log_returns.index[-1])
log_returns.index = closing_price.index[0:-1]

# Calculate the correlation matrix
corr_matrix = log_returns.corr()




