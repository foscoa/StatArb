import pandas as pd
import pymongo
import pandas as pd
import numpy as np


# Create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# https://markets.newyorkfed.org/static/docs/markets-api.html#/Reference%20Rates
json_rf = pd.read_json("https://markets.newyorkfed.org/api/rates/all/search.json?startDate=2001-01-01&endDate=2023-05-01")

rf_dates = [item['effectiveDate'] for item in json_rf.refRates]
rf_dates = np.unique(rf_dates)

json_rf_to_write = {}

for j in rf_dates:
    temp_dict = {}
    for k in json_rf.refRates:
        dict((key, k[key]) for key in ['type', 'percentRate'])


