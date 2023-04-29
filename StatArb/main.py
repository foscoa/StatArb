import numpy as np
import pandas as pd
import yfinance as yf
import statsmodels.api as sm

import matplotlib.pyplot as plt

def calc_zscore(series):
    return (series - np.mean(series)) / np.std(series)


### equities data - download and visualization -------------------------------------------------------------------------

STOCK_A = "AAPL"
STOCK_B = "MSFT"
stock_a = yf.download(STOCK_A, start='2020-01-01', end='2023-04-28')
stock_b = yf.download(STOCK_B, start='2020-01-01', end='2023-04-28')

# dataframe with closing price
closing_price = pd.concat([stock_a['Close'], stock_b['Close']], axis=1)
closing_price.columns = [STOCK_A, STOCK_B]

# show the closing price plot
closing_price.plot.line()
# plt.show()

### --------------------------------------------------------------------------------------------------------------------

### model - linear regression ------------------------------------------------------------------------------------------

model = sm.OLS(closing_price[STOCK_A],
               sm.add_constant(closing_price[STOCK_B])
               ).fit()

# get intercept and slope from model parameters
intercept, slope = model.params

# plot scatter plot of data
plt.scatter(stock_b['Close'], closing_price[STOCK_A])

# plot regression line
plt.plot(closing_price[STOCK_B], intercept + slope * closing_price[STOCK_B], color='red')

# set plot title and axis labels
plt.title('Regression plot')
plt.xlabel('b_close')
plt.ylabel('a_close')

# show the plot
# plt.show()

# Calculate the predicted values of A based on B using the fitted model
a_pred = model.predict(sm.add_constant(closing_price[STOCK_B]))

# Calculate the residuals between the predicted and actual values of A
residuals = closing_price[STOCK_A] - a_pred

# Calculate the Z-score of the residuals
zscore = (residuals - residuals.mean()) / residuals.std()

# Set the threshold for the Z-score
threshold = 1.5




### --------------------------------------------------------------------------------------------------------------------







