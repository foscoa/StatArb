# here goes the code to dowload fil from the SEC

import pandas as pd
import json
import requests
import bs4 as bs


file_name = 'company_2023QTR2.idx'
dir_SEC = "C:\\Users\\Fosco\\Desktop\\"

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

res = requests.get(filing_name,
                   headers={'User-Agent': 'Fosco Antognini',
                           'Accept-Encoding': 'gzip, deflate',
                           'Host': 'www.sec.gov'})

# Parse the response HTML using BeautifulSoup
soup = bs.BeautifulSoup(res.text, "lxml")

# Extract all tables from the response
html_tables = soup.find_all('table')
