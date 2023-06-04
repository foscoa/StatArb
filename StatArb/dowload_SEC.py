# here goes the code to dowload fil from the SEC

import pandas as pd
import json
import requests
import bs4 as bs
import numpy as np

# https://www.sec.gov/Archives/edgar/full-index/2023/QTR2/company.idx
file_name = 'company_2023QTR2.idx'
dir_SEC = '/Users/foscoantognini/Desktop/'   # home
# dir_SEC = "C:\\Users\\Fosco\\Desktop\\"     # office

def parseCompanySECfiles(filename):
    idx_table = pd.read_table(filename)

    # Parse data into a list of dictionaries
    data = []
    for line in list(idx_table.iloc[5:, :].values):

        temp_string = str(line[0])
        temp_array = [element for element in temp_string.split('  ') if element != '' and (not element.isspace())]

        data.append({
            "Company Name": temp_array[0].strip(),
            "Form Type": temp_array[1].strip(),
            "CIK": temp_array[2].strip(),
            "Date Filed": temp_array[3].strip(),
            "File Name": temp_array[4].strip()
        })

    # Create DataFrame
    df = pd.DataFrame(data)

    return df

def readSECTickersExchangeFile(filename):
    # Read JSON file
    with open(filename, 'r') as file:
        json_data = json.load(file)

    # Extract fields and data from JSON
    fields = json_data["fields"]
    data = json_data["data"]

    # Create DataFrame
    return pd.DataFrame(data, columns=fields)

# parse company file to get all filing names
df_filings_names = parseCompanySECfiles(dir_SEC + file_name)

# find AMZN 10-Q filing for 2023QTR2
ticker = 'AMZN'
company_tickers_exchange = readSECTickersExchangeFile(dir_SEC + "company_tickers_exchange.json")
CIK = str(company_tickers_exchange[company_tickers_exchange.ticker == ticker].cik.values[0])

filing_name = 'https://www.sec.gov/Archives/' +\
              df_filings_names[(df_filings_names['CIK'] == CIK) &
                               (df_filings_names['Form Type'] == '10-Q')].values[0][-1]

# scrape 10-Q SEC filings
# https://quantopian-archive.netlify.app/notebooks/notebooks/quantopian_notebook_474.html
# https://www.sec.gov/ix?doc=/Archives/edgar/data/1018724/000101872423000008/amzn-20230331.htm
# https://www.sec.gov/Archives/edgar/data/1018724/0001018724-23-000008.txt

res = requests.get(filing_name,
                   headers={'User-Agent': 'Fosco Antognini',
                           'Accept-Encoding': 'gzip, deflate',
                           'Host': 'www.sec.gov'})

# Parse the response HTML using BeautifulSoup
soup = bs.BeautifulSoup(res.text, "lxml")

# Extract all tables from the response
html_tables = soup.find_all('table')

# Iterate over the tables and find Statement of Cash Flows (SCF)"
for table in html_tables:
    # Convert the table to a string and check for the word
    if 'CASH, CASH EQUIVALENTS, AND RESTRICTED CASH, BEGINNING OF PERIOD' in str(table):
        # Process the table or perform desired operations
        SCF = pd.read_html(str(table))[0]

# get only last 3 months ended CF and remove first row, abstract
SCF = SCF.iloc[1:, 0:2]

CF_json = {}    # empty json to store clean
multiplier = 0
if 'in Millions' in str(SCF.columns[0]): multiplier = 1000

# Divide dataframe
arr = np.array([str(item).isupper() for item in SCF.iloc[:, 0]])

# Find the positions where the array is True
positions = np.where(arr)[0]
positions = np.append(positions, len(arr))

for j in range(0, (len(positions)-1)):
    # take care of single entries
    diff = positions[j+1] - positions[j]
    if diff == 1:
        CF_json[str(SCF.iloc[positions[j]:positions[j + 1], 0].values[0])] =\
            int(str(SCF.iloc[positions[j]:positions[j+1], 1].values[0]).replace('$', '').replace(',', '').split()[0])*multiplier
    else:
        # Select decomposition
        temp_sub_df = SCF.iloc[positions[j]:positions[j+1], :]
        sub_dict = {}
        for k in range(1, temp_sub_df.shape[0]):
            if type(temp_sub_df.iloc[k, 1]) == str:
                sub_dict[temp_sub_df.iloc[k,0]] =\
                    int(temp_sub_df.iloc[k,1].replace('(', '-').replace(',', '').replace(')', ''))*multiplier
            else:
                sub_dict[temp_sub_df.iloc[k, 0]] = temp_sub_df.iloc[k, 1]


        CF_json[temp_sub_df.iloc[0,0]] = sub_dict











# Parse the Filings table
filings_table = pd.read_html(str(html_tables[2]))[0]
