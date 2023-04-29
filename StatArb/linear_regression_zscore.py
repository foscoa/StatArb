import numpy as np
import pandas as pd
import yfinance as yf
import statsmodels.api as sm
import plotly.express as px

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

# plot prices
# fig = px.line(closing_price)
# fig.show()

# plot regression line
# fig = px.scatter(closing_price, x=STOCK_B, y=STOCK_A, trendline="ols")
# fig.show()

# Calculate the predicted values of A based on B using the fitted model
a_pred = model.predict(sm.add_constant(closing_price[STOCK_B]))

# Calculate the residuals between the predicted and actual values of A
residuals = closing_price[STOCK_A] - a_pred

# Calculate the Z-score of the residuals
zscore = (residuals - residuals.mean()) / residuals.std()

# Set the threshold for the Z-score
threshold = 1.5

# Calculate the long and short positions based on the Z-score and threshold
signals = np.where(zscore > threshold, -1,
                   np.where(zscore < -threshold, 1,
                            0))

### --------------------------------------------------------------------------------------------------------------------

### Calculate the P&L series -------------------------------------------------------------------------------------------

pnl = signals * residuals.shift(-1)
cum_pnl = pnl.cumsum()

# plot cumulative pnl
fig = px.line(cum_pnl)
fig.show()

### --------------------------------------------------------------------------------------------------------------------







