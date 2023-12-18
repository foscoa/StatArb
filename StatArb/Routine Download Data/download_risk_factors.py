import requests
import zipfile
import io
import csv
import os
from datetime import datetime
import pymongo

def download_and_read_csv(url):
    response = requests.get(url)
    zip_file = 'temp.zip'  # Specify the path and filename to save the downloaded ZIP file
    list_zip = []

    with open(zip_file, 'wb') as file:
        file.write(response.content)

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_names = zip_ref.namelist()  # Get a list of file names inside the ZIP file
        for file_name in file_names:
            with zip_ref.open(file_name) as file:
                csv_data = io.TextIOWrapper(file)  # Wrap the file with a text I/O wrapper
                csv_reader = csv.reader(csv_data)
                for row in csv_reader:
                    # Process each row of the CSV data
                    list_zip.append(row)

    # Clean up the temporary ZIP file
    os.remove(zip_file)

    return list_zip

# Create a mongodb client, use default local host
try:
    client = pymongo.MongoClient()
except Exception:
    print("Error: " + Exception)

# select the collection
collection = client.Financial_Data.Risk_Factors

# Usage example:
url_f = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/North_America_3_Factors_Daily_CSV.zip'
url_m = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/North_America_Mom_Factor_Daily_CSV.zip'
factors_f = download_and_read_csv(url_f)
factor_m = download_and_read_csv(url_m)

factors_names = ['Mkt-RF', 'SMB', 'HML', 'RF']
factors_data = factors_f[factors_f.index(['', 'Mkt-RF', 'SMB', 'HML', 'RF'])+1:]
factors_data.reverse()

mom_data = factor_m[factor_m.index(['', 'WML'])+1:]

for i in factors_data:
    curr_date = datetime.strptime(i[0].replace(' ',''), '%Y%m%d').isoformat()

    if len(list(collection.find({'timestamp': curr_date}))) == 0: # check if date already exists

        temp_dict = {}
        temp_dict['timestamp'] = curr_date
        counter = 1
        for j in factors_names:
            temp_dict[j] = float(i[counter].replace(' ',''))
            counter += 1
        #integrate mom
        bools = [k[0].replace(' ', '') == i[0].replace(' ', '') for k in mom_data]
        if sum(bools) > 0:
            pos = [index for index, value in enumerate(bools) if value][0]
            temp_dict['WML'] = float(mom_data[pos][1].replace(' ',''))
        else:
            temp_dict['WML'] = 'nan'

        collection.insert_one(temp_dict)
