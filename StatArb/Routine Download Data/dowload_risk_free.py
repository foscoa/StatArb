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

start_date ='2023-05-16'
end_date = datetime.today().strftime("%Y-%m-%d")

# Retrieve all days
# days = list(collection.distinct("timestamp"))

# both start and end included
json_rf = pd.read_json("https://markets.newyorkfed.org/api/rates/all/search.json?startDate=" +
                       start_date +
                       "&endDate=" +
                       end_date )

rf_dates = [item['effectiveDate'] for item in json_rf.refRates]
rf_dates = np.unique(rf_dates)

for date in rf_dates:
    temp_list = []
    for item_dict in json_rf.refRates:
        # filet for SOFR or EFFR:
        if (item_dict['effectiveDate'] == date) and (('SOFR' in item_dict.values()) or ('EFFR' in item_dict.values())):
            temp_list.append(dict((key, item_dict[key]) for key in ['type', 'percentRate']))

    date_obj = datetime.strptime(date, '%Y-%m-%d')
    iso_date = date_obj.isoformat()
    collection.insert_one({'timestamp': iso_date, 'Data': temp_list})




