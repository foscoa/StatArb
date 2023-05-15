import pymongo

# Step 1 - Install and import pymongo
import pymongo

# Step 2 - create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# select the collection
collection = client.Financial_Data.Daily_Timeseries

quer_result = collection.find({"Data.Symbol": "AAPL"})

print(list(quer_result))





