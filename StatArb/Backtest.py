# Step 1 - Install and import pymongo
import calendar
from datetime import datetime

import matplotlib
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pymongo
import seaborn as sns
from dash import Dash, html, dcc, dash_table
from dash.dash_table import FormatTemplate
from plotly.subplots import make_subplots
from StatArb.utils.query_mongoDB_functions import *
from StatArb.utils.routine_functions import *


app = Dash(__name__)

# Step 2 - create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)


use_mongoDB = False

if use_mongoDB == True:

    # select the collection
    collection_equity = client.Financial_Data.Daily_Timeseries
    collection_rates = client.Financial_Data.Risk_Free
    collection_BM = client.Financial_Data.Indices
    collection_FCT = client.Financial_Data.Risk_Factors

    # Specify the symbols you want to retrieve the time series for
    symbols = ['CVX', 'STZ']
    benchmark = ['^SPX']
    start = '2015-01-02'
    end = '2023-01-16'
    print("Last day in time series is " + str(list(collection_equity.distinct("timestamp"))[-1]))
    param = 'Adj Close'

    price_TS = queryTimeSeriesEquity(symbols      = symbols,
                                     start        = start,
                                     end          = end,
                                     param        = param,
                                     collection   = collection_equity)

    rates_TS = queryTimeSeriesRates(start         = start,
                                    end           = end,
                                    collection    = collection_rates)

    factors_TS = queryTimeSeriesFactors(start         = start,
                                        end           = end,
                                        collection    = collection_FCT)

    price_BM = queryTimeSeriesEquity(symbols      = benchmark,
                                     start        = start,
                                     end          = end,
                                     param        = param,
                                     collection   = collection_BM)

    # Merge the two DataFrames based on a common time index
    EFFR_TS = pd.DataFrame(pd.merge(price_TS , rates_TS, left_index=True, right_index=True, how='left')['EFFR'])
    # Interpolate the missing values
    EFFR_TS = EFFR_TS.interpolate()
    # Smooth the values using a simple moving average
    # window_size = 10
    # EFFR_smoothed = EFFR_TS.rolling(window_size).mean()
    # EFFR_smoothed['EFFR'][0:(window_size-1)] = EFFR_TS['EFFR'][0:(window_size-1)]
    EFFR_TS = EFFR_TS/(100*252)


    # Merge the two DataFrames based on a common time index
    BM_TS = pd.DataFrame(pd.merge(price_TS, price_BM, left_index=True, right_index=True, how='left')['^SPX'])
    # Interpolate the missing values
    BM_TS = BM_TS.interpolate()
    # Delete first element
    BM_TS = BM_TS[1:]

else:
    # dir_data = "C:\\Users\\Fosco\\Desktop\\Sample data\\"
    dir_data = "/Users/foscoantognini/Documents/StatArb_input/"
    price_TS = pd.read_csv(dir_data + "stock_prices.csv", parse_dates=['Date'], index_col='Date')
    rates_TS = pd.read_csv(dir_data + "rates.csv", parse_dates=['Date'], index_col='Date')
    BM_TS = pd.read_csv(dir_data+ "BM.csv", parse_dates=['Date'], index_col='Date')

    price_TS.drop('VATE', inplace=True, axis=1) # VATE has negative prices
    # rates_TS = rates_TS / (252 * 100)

    # Merge the two DataFrames based on a common time index
    EFFR_TS = pd.DataFrame(pd.merge(price_TS , rates_TS, left_index=True, right_index=True, how='left')['EFFR'])
    # Interpolate the missing values
    EFFR_TS = EFFR_TS.interpolate()
    # Smooth the values using a simple moving average
    # window_size = 10
    # EFFR_smoothed = EFFR_TS.rolling(window_size).mean()
    # EFFR_smoothed['EFFR'][0:(window_size-1)] = EFFR_TS['EFFR'][0:(window_size-1)]
    EFFR_TS = EFFR_TS/(100*252)

# signal, long CVX short STZ
signal = price_TS*0

def generateRandomSignal(signal):

    # Generate random values of -1, 0, and 1
    random_values = np.random.choice([-0.01, 0, 0.01], size=(signal.shape[0]-1, signal.shape[1]))

    # Create a DataFrame
    random_signal = pd.DataFrame(random_values, columns=signal.columns)
    random_signal.index = signal.index[1:]

    return random_signal

signal = generateRandomSignal(signal)
# signal[symbols[0]] += 0.5
# signal[symbols[1]] += 1

class BacktestTradingStrategy:
    def __init__(self,
                 name: str,
                 description: str,
                 asset_prices,
                 risk_free      = np.nan,
                 benchmark      = np.nan,
                 signal         = np.nan):

        # run validations to the received arguments
        assert asset_prices[1:].index.equals(signal.index), "The input - asset_prices - (with the first " \
                                                            "row deleted) and the input - signal - do not " \
                                                            "have the same indices."

        assert asset_prices.index.equals(risk_free.index), "The input - asset_prices - (with the first " \
                                                            "row deleted) and the input - risk_free - do not " \
                                                            "have the same indices."

        assert asset_prices.index.equals(benchmark.index), "The input - asset_prices - (with the first " \
                                                               "row deleted) and the input - benchmark - do not " \
                                                               "have the same indices."



        # assign to self object
        self.name = name
        self.description = description
        self.asset_prices = asset_prices
        self.risk_free = risk_free
        self.benchmark = benchmark
        self.signal = signal



    def portfolio_log_returns(self):

        PT_log_returns = self.signal * calculate_log_returns(self.asset_prices)
        PT_log_returns.fillna(0, inplace=True)
        PT_log_returns = pd.DataFrame(data=PT_log_returns.sum(axis=1), columns=['Portfolio'])

        return PT_log_returns

    def portfolio_cumulative_log_returns(self):

        PT_log_returns = self.signal * calculate_log_returns(self.asset_prices)
        PT_log_returns.fillna(0, inplace=True)
        PT_log_returns = pd.DataFrame(data=PT_log_returns.sum(axis=1), columns=['Portfolio'])
        PT_cum_ret = (PT_log_returns+1).cumprod()

        return PT_cum_ret

    def portfolio_monthly_returns(self):
        # development ground
        PT_cum_ret = self.portfolio_cumulative_log_returns()
        monthly_returns = {}

        for year in PT_cum_ret.index.year.unique():
            PT_cum_ret_year = PT_cum_ret[PT_cum_ret.index.year == year]
            for month in PT_cum_ret_year.index.month.unique():
                PT_cum_ret_month = PT_cum_ret_year[PT_cum_ret_year.index.month == month]
                monthly_returns[calendar.month_abbr[month] + "-" + str(year)] = \
                    PT_cum_ret_month['Portfolio'][-1] / PT_cum_ret_month['Portfolio'][0] - 1

        return monthly_returns

    def portfolio_YTD_returns(self):
        # development ground
        PT_cum_ret = self.portfolio_cumulative_log_returns()
        YTD_returns = {}

        for year in PT_cum_ret.index.year.unique():
            PT_cum_ret_year = PT_cum_ret[PT_cum_ret.index.year == year]

            YTD_returns["YTD-" + str(year)] = \
                    PT_cum_ret_year['Portfolio'][-1] / PT_cum_ret_year['Portfolio'][0] - 1

        return YTD_returns

    def portfolio_monthly_returns_table(self):
        # develop
        PT_cum_ret = self.portfolio_cumulative_log_returns()
        monthy_returns_table = pd.DataFrame(data=[],
                                            columns=calendar.month_abbr[1:13] + ["YTD"],
                                            index=PT_cum_ret.index.year.unique()[::-1])

        for year in PT_cum_ret.index.year.unique():
            PT_cum_ret_year = PT_cum_ret[PT_cum_ret.index.year == year]
            monthy_returns_table["YTD"][year] = PT_cum_ret_year['Portfolio'][-1] / PT_cum_ret_year['Portfolio'][0] - 1
            for month in PT_cum_ret_year.index.month.unique():
                PT_cum_ret_month = PT_cum_ret_year[PT_cum_ret_year.index.month == month]
                monthy_returns_table[calendar.month_abbr[month]][year] = \
                    PT_cum_ret_month['Portfolio'][-1] / PT_cum_ret_month['Portfolio'][0] - 1

        return monthy_returns_table

    def drawdown(self):

        PT_cum_ret = self.portfolio_cumulative_log_returns()
        DD = (PT_cum_ret - PT_cum_ret.cummax())

        return DD

    def drawdown_prc(self):

        PT_cum_ret = self.portfolio_cumulative_log_returns()
        DD = (PT_cum_ret/PT_cum_ret.cummax()) - 1

        return DD

    def cagr(self):

        PT_cum_ret = self.portfolio_cumulative_log_returns()

        days = PT_cum_ret.index[-1] - PT_cum_ret.index[0]
        years = days.days/365

        cagr = (float(PT_cum_ret.values[-1])**(1/years))-1

        return cagr

    def calculate_summary_statistics(self):

        summary_stat = {}

        PT_log_returns = self.portfolio_log_returns()

        # TODO: reintroduce subtraction risk-free rate
        PT_excess_returns = pd.DataFrame(PT_log_returns['Portfolio'] - self.risk_free['EFFR'])
        # PT_excess_returns = PT_log_returns

        summary_stat["ann. mean"] = float(PT_log_returns.mean().values) * 252
        summary_stat["ann. std"] = float(PT_log_returns.std().values) * np.sqrt(252)
        summary_stat["max DD"] = float(self.drawdown_prc().min().values)
        summary_stat["sharpe ratio"] = float(PT_excess_returns.mean().values)*252/\
                                       (float(PT_log_returns.std().values) * np.sqrt(252))
        summary_stat["CAGR"] = self.cagr()

        return summary_stat

    def generate_report(self):

        # benchmark returns
        BM_log_returns = calculate_log_returns(self.benchmark)
        BM_log_returns.fillna(0, inplace=True)
        BM_cum_log_returns = pd.DataFrame(data=BM_log_returns.sum(axis=1) + 1, columns=['Bemchmark']).cumprod()

        # portfolio returns
        PT_returns = self.portfolio_cumulative_log_returns()

        fig = make_subplots(rows=2,
                            cols=1,
                            shared_xaxes=True,
                            vertical_spacing=0.02,
                            row_heights=[0.8, 0.2]
                            )

        fig.add_trace(
            go.Scatter(
                x=PT_returns.index.to_list(),
                y=PT_returns['Portfolio'].to_list(),
                line_color='cadetblue',
                name='Strategy'
            ),
            row=1,
            col=1)

        fig.add_trace(
            go.Scatter(
                x=BM_cum_log_returns.index.to_list(),
                y=BM_cum_log_returns['Bemchmark'].to_list(),
                line_color='tan',
                name='S&P500'
            ),
            row=1,
            col=1)

        fig.add_trace(
            go.Scatter(
                x=self.drawdown_prc().index.to_list(),
                y=self.drawdown_prc()['Portfolio'].to_list(),
                fill='tozeroy',
                line_color="darkred",
                name='Drawdown'
            ),
            row=2,
            col=1)

        fig.update_yaxes(tickformat='.2%',
                         row=2,
                         col=1)

        fig.update_layout(
            title=strategy.name + "<br>"
                  + "<sub>"
                  + "CAGR = " + str(round(self.calculate_summary_statistics()['CAGR'] * 100, 2)) + "%,   "
                  + "mean p.a. = " + str(round(self.calculate_summary_statistics()['ann. mean'] * 100, 2)) + "%,   "
                  + "std p.a. = " + str(round(self.calculate_summary_statistics()['ann. std'] * 100, 2)) + "%,   "
                  + "max DD = " + str(round(self.calculate_summary_statistics()['max DD'] * 100, 2)) + "%,   "
                  + "sharpe ratio = " + str(round(self.calculate_summary_statistics()['sharpe ratio'], 2)) +

                  " </sub>" +

                  "<br>",

            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        return fig

    def generate_dash_monthly_returns_table(self):

        my_df = self.portfolio_monthly_returns_table()
        my_df.insert(0, 'Year', my_df.index)
        percentage = FormatTemplate.percentage(2)

        n_palette = 40
        min_color = -0.3
        max_color = 0.3

        master_palette = [matplotlib.colors.to_hex(color) for color in
                          sns.diverging_palette(h_neg=10, h_pos=120, l=50, s=100, n=n_palette + 1, as_cmap=False)]

        filter = np.linspace(start=min_color, stop=max_color, num=len(master_palette) - 1)
        filter = np.insert(filter, 0, -1)
        filter = np.append(filter, 10)

        return dash_table.DataTable(
                    data=my_df.to_dict('records'),
                    columns=[{'id': my_df.columns[0], 'name': my_df.columns[0]}] +
                            [{'id': c, 'name': c, 'type': 'numeric', 'format': percentage} for c in my_df.columns[1:]],
                    css=[{'selector': 'table', 'rule': 'table-layout: fixed'}],
                    style_cell={
                        'width': '{}%'.format(len(my_df.columns))
                    },
                    style_data_conditional=[
                        {
                            'if': {
                                'column_id': 'YTD'
                            },
                            'fontWeight': 'bold',
                            'border-left': 'double'
                        },
                        {
                            'if': {
                                'column_id': 'Year'
                            },
                            'fontWeight': 'bold',
                            'border-right': 'double'
                        },

                    ] + [
                        {
                            'if': {
                                'filter_query': '{'+ j +'} > ' + str(filter[i]) + ' && {'+ j +'} < ' + str(filter[i+1])
                                                + ' && {' + j + '} !=0',
                                'column_id': j},
                            'backgroundColor': str(master_palette[i])
                        } for i in range(0, n_palette+1) for j in strategy.portfolio_monthly_returns_table().columns
                    ]


                )


# Create an instance of the TradingStrategy class
strategy = BacktestTradingStrategy(
    name="Random Strategy",
    description="Invest in high-performing assets over a certain period of time",
    asset_prices=price_TS,
    risk_free=EFFR_TS,
    benchmark=BM_TS,
    signal=signal
)


# develop

app.layout = html.Div(
    children=[
        html.Div(
            children=[
                dcc.Graph(
                    id='example-graph',
                    figure=strategy.generate_report(),
                    style= {'height': '70vh'}
                ),
            ],
            style={'height': '70vh'}
        ),

        html.Div(
            children=[
                strategy.generate_dash_monthly_returns_table()
            ],
            style={'marginLeft': 80, 'marginRight': 120}
        ),

    ]
)

if __name__ == '__main__':
    app.run_server(debug=False, port=5000)













