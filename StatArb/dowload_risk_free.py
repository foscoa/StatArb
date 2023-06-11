import pandas as pd
import pymongo
import pandas as pd
import numpy as np
from datetime import datetime


# Create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# select the collection
collection = client.Financial_Data.Risk_Free

# https://markets.newyorkfed.org/static/docs/markets-api.html#/Reference%20Rates
json_rf = pd.read_json("https://markets.newyorkfed.org/api/rates/all/search.json?startDate=2001-01-01&endDate=2023-05-01")

rf_dates = [item['effectiveDate'] for item in json_rf.refRates]
rf_dates = np.unique(rf_dates)

for date in rf_dates:
    temp_list = []
    for k in json_rf.refRates:
        # filet for SOFR or EFFR:
        if ('SOFR' in k.values()) or ('EFFR' in k.values()):
            temp_list.append(dict((key, k[key]) for key in ['type', 'percentRate']))

    date_obj = datetime.strptime(date, '%Y-%m-%d')
    iso_date = date_obj.isoformat()
    collection.insert_one({iso_date:temp_list})




