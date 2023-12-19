import pandas as pd
import yfinance as yf
import numpy as np
import pandas_ta
import pandas_datareader.data as web

end_date = '2023-09-27'
start_date = pd.to_datetime(end_date) - pd.DateOffset(365 * 8)

def download_yf_sp500(start_date, end_date):
    sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]

    sp500['Symbol'] = sp500['Symbol'].str.replace('.', '-')

    symbols_list = sp500['Symbol'].unique().tolist()

    df = yf.download(tickers=symbols_list,
                     start=start_date,
                     end=end_date).stack()

    df.index.names = ['date', 'ticker']

    df.columns = df.columns.str.lower()

    return df

sp500_data = download_yf_sp500(start_date, end_date)

sp500_data['garman_klass_vol'] = ((np.log(sp500_data['high'])-np.log(sp500_data['low']))**2)/2\
                                 - (2*np.log(2)-1)*((np.log(sp500_data['adj close'])-np.log(sp500_data['open']))**2)

sp500_data['rsi'] = sp500_data.groupby(level=1)['adj close'].transform(lambda x: pandas_ta.rsi(close=x, length=20))

sp500_data['bb_low'] = sp500_data.groupby(level=1)['adj close'].transform(
    lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:, 0])

sp500_data['bb_mid'] = sp500_data.groupby(level=1)['adj close'].transform(
    lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:, 1])

sp500_data['bb_high'] = sp500_data.groupby(level=1)['adj close'].transform(
    lambda x: pandas_ta.bbands(close=np.log1p(x), length=20).iloc[:, 2])

def compute_atr(stock_data):
    atr = pandas_ta.atr(high=stock_data['high'],
                        low=stock_data['low'],
                        close=stock_data['close'],
                        length=14)
    return atr.sub(atr.mean()).div(atr.std())

sp500_data['atr'] = sp500_data.groupby(level=1, group_keys=False).apply(compute_atr)

def compute_macd(close):
    macd = pandas_ta.macd(close=close, length=20).iloc[:,0]
    return macd.sub(macd.mean()).div(macd.std())

sp500_data['macd'] = sp500_data.groupby(level=1, group_keys=False)['adj close'].apply(compute_macd)

sp500_data['dollar_volume'] = (sp500_data['adj close']*sp500_data['volume'])/1e6


# Aggregate to monthly level and filter top 150 most liquid stocks for each month.¶
# To reduce training time and experiment with features and strategies, we convert the business-daily data to month-end frequency

last_cols = [c for c in sp500_data.columns.unique(0) if c not in ['dollar_volume', 'volume', 'open',
                                                          'high', 'low', 'close']]

data = (pd.concat([sp500_data.unstack('ticker')['dollar_volume'].resample('M').mean().stack('ticker').to_frame('dollar_volume'),
                   sp500_data.unstack()[last_cols].resample('M').last().stack('ticker')],
                  axis=1)).dropna()

# Calculate 5-year rolling average of dollar volume for each stocks before filtering. The rolling mean is calculated with
# a window size of 3, but the min_periods parameter is set to 2. This means that if there are at least 2 non-null
# observations in the window, the rolling mean will be computed; otherwise, it will be NaN.

data['dollar_volume'] = (data.loc[:, 'dollar_volume'].unstack('ticker').rolling(5*12, min_periods=12).mean().stack())

data['dollar_vol_rank'] = (data.groupby('date')['dollar_volume'].rank(ascending=False))

data = data[data['dollar_vol_rank'] < 150].drop(['dollar_volume', 'dollar_vol_rank'], axis=1)

# 4. Calculate Monthly Returns for different time horizons as features.
# To capture time series dynamics that reflect, for example, momentum patterns, we compute historical
# returns using the method .pct_change(lag), that is, returns over various monthly periods as identified by lags.

def calculate_returns(df):
    outlier_cutoff = 0.005

    lags = [1, 2, 3, 6, 9, 12]

    for lag in lags:
        df[f'return_{lag}m'] = (df['adj close']
                                .pct_change(lag)
                                .pipe(lambda x: x.clip(lower=x.quantile(outlier_cutoff),
                                                       upper=x.quantile(1 - outlier_cutoff)))
                                .add(1)
                                .pow(1 / lag)
                                .sub(1))
    return df


data = data.groupby(level=1, group_keys=False).apply(calculate_returns).dropna()

#5. Download Fama-French Factors and Calculate Rolling Factor Betas.
# We will introduce the Fama—French data to estimate the exposure of assets to common risk factors using linear regression.
# The five Fama—French factors, namely market risk, size, value, operating profitability, and investment have been shown
# empirically to explain asset returns and are commonly used to assess the risk/return profile of portfolios. Hence, it
# is natural to include past factor exposures as financial features in models.
# We can access the historical factor returns using the pandas-datareader and estimate historical exposures using the
# RollingOLS rolling linear regression.

factor_data = web.DataReader('F-F_Research_Data_5_Factors_2x3',
                               'famafrench',
                               start='2010')[0].drop('RF', axis=1)

factor_data.index = factor_data.index.to_timestamp()

factor_data = factor_data.resample('M').last().div(100)

factor_data.index.name = 'date'

factor_data = factor_data.join(data['return_1m']).sort_index()

factor_data