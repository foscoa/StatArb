import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats


data_directory = "C:\\Users\\Fosco\\Desktop\\Fosco\\VIX_Data\\"

# Get a list of all files in the directory
file_list = os.listdir(data_directory)

# Create an empty DataFrame with a multi-index
VIX_fut_data = pd.DataFrame()

# Iterate through CSV files in the specified directory
for csv_file in file_list:
    # Read each CSV file into a DataFrame
    VIX_fut_data = pd.concat([VIX_fut_data.copy(),
                             pd.read_csv(data_directory + csv_file,
                                        parse_dates=['Trade Date'],
                                        index_col=['Trade Date', 'Futures'])
                             ])


# Sort the multi-index by level 0 (timestamp) for better readability
VIX_fut_data.sort_index(axis=0, level=0, inplace=True)

# rename indices
VIX_fut_data.index.names = ['Date', 'Contract']

# Get the first level as a list
contracts = VIX_fut_data.index.get_level_values('Contract').unique().to_list()

# Print number of contracts available
print(f'There are {len(contracts)} futures contracts in the dataset.\n')

# Check if there are all the for each date and in increasing order
date_contracts = [(lambda x: x[x.find('(') + 1:x.find(')')])(c) for c in contracts]
date_contracts = [datetime.strptime(dt_c, '%b %Y') for dt_c in date_contracts]
delta_time = np.diff(date_contracts)
assert len([contracts[i] for i in np.where((delta_time > timedelta(days=31)))[0]]) == 0, 'There are some contracts missing'

# check if there are index duplicates
assert VIX_fut_data[VIX_fut_data.index.duplicated()].empty, 'There are duplicates in the index'

# drop unwanted columns
columns_to_keep = ['Close', 'Total Volume', 'Open Interest']
VIX_fut_data.drop([c for c in VIX_fut_data.columns if c not in columns_to_keep], inplace=True, axis=1)

VIX_fut_data['Timestamp Contract'] = [(lambda x: datetime.strptime(x[x.find('(') + 1:x.find(')')], '%b %Y'))(c)
                                      for c in VIX_fut_data.index.get_level_values('Contract')]

# remove contracts with 0 Close
VIX_fut_data = VIX_fut_data.copy()[VIX_fut_data.Close > 0]
print('Removed rows where Close is zero.\n')

# remove contracts with Volume below 20% quantile
low_volume_tsld = VIX_fut_data['Total Volume'].quantile(0.20)
VIX_fut_data = VIX_fut_data.copy()[VIX_fut_data['Total Volume'] > low_volume_tsld]
print(f'Removed rows where Total Volume is less than {round(low_volume_tsld)}.\n')

# Explore database
print(VIX_fut_data.info())
print(VIX_fut_data.describe())

# Counting contracts for each group
contract_counts = VIX_fut_data.groupby('Timestamp Contract').count()
# plt.plot(contract_counts.index, contract_counts.Close)

# Based on evidence, we decide to keep contracts from Aug 2013 until Dec 2013
VIX_fut_data = VIX_fut_data.copy()[VIX_fut_data['Timestamp Contract'].between('2013-08', '2023-12')]

# Divide the data in Train set and Validation set
all_contracts = VIX_fut_data.index.get_level_values('Contract').unique().to_list()
train_prc = 0.8
train_contracts = all_contracts[0:int(round(len(all_contracts)*0.8))]
valid_contracts = all_contracts[int(round(len(all_contracts)*0.8)):]

VIX_fut_data_train = VIX_fut_data[[idx in train_contracts for idx in VIX_fut_data.index.get_level_values('Contract')]]
VIX_fut_data_valid = VIX_fut_data[[idx in valid_contracts  for idx in VIX_fut_data.index.get_level_values('Contract')]]

# Calculate returns
def calculate_returns(df, colname, type):
    if type == 'log':
        return np.log(df[colname]).diff()
    elif type == 'pct':
        return df[colname].pct_change()
    else:
        raise ValueError("type should be either 'log' or 'pct'")

# add returns columns to the data
VIX_fut_data_train['Log Returns'] = (VIX_fut_data_train.copy().groupby('Contract', group_keys=False)\
                                                      .apply(calculate_returns, colname='Close', type='log'))
VIX_fut_data_train.dropna(inplace=True, axis=0)


# investigate distribution of returns
log_returns = VIX_fut_data_train['Log Returns']
print(log_returns.describe()) # box plot
def QQplot(df, dist):
    # Create QQ plot
    fig, ax = plt.subplots(figsize=(8, 6))
    stats.probplot(df, dist=dist, plot=ax)

    # Customize the plot
    ax.set_title("QQ Plot")
    ax.set_xlabel("Theoretical Quantiles")
    ax.set_ylabel("Sample Quantiles")

    # Show the plot
    plt.show()
# QQplot(log_returns, dist='norm').show()



