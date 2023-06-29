import numpy as np

def calculate_log_returns(asset_prices):
    # calculate log returns
    log_returns = np.log(asset_prices).shift(-1) - np.log(asset_prices)
    log_returns = log_returns.drop(log_returns.index[-1])
    log_returns.index = asset_prices.index[1:]

    return log_returns
