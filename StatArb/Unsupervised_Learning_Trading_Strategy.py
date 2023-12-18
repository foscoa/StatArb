import pandas as pd
import yfinance as yf
import numpy as np
import pandas_ta

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

