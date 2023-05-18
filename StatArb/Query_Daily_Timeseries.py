import pymongo

# Step 1 - Install and import pymongo
import pymongo
import pandas as pd
import plotly.express as px

# Step 2 - create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# select the collection
collection = client.Financial_Data.Daily_Timeseries

def generateTimeSeries(symbol, variable, collection):

    # Specify the symbols you want to retrieve the time series for
    symbols = ['CVX', 'STZ']

    # Define the aggregation pipeline
    pipeline = [
        {'$match': {'Data.Symbol': {'$in': symbols}}},
        {'$unwind': '$Data'},
        {'$match': {'Data.Symbol': {'$in': symbols}}},
        {'$group': {'_id': '$timestamp', 'Data': {'$push': '$Data'}}},
        {'$project': {'_id': 0, 'timestamp': '$_id', 'Data': 1}}
    ]

    # Execute the aggregation pipeline
    results = collection.aggregate(pipeline)

    # Extract the data for each symbol and create a Pandas DataFrame
    data = {}
    for symbol in symbols:
        data[symbol] = []

    for result in results:
        timestamp = result['timestamp']
        for entry in result['Data']:
            if entry['Symbol'] in symbols:
                data[entry['Symbol']].append((timestamp, entry['Adj Close']))

    # Create a Pandas DataFrame from the extracted data
    df = pd.DataFrame(data)

    # Set the timestamp as the index
    df.set_index('timestamp', inplace=True)

    # Print the resulting time series
    return df

cvx = generateTimeSeries('CVX')

# plot cumulative pnl
fig = px.line(cvx)
fig.show()





