import csv, time
import pandas as pd
import numpy as np

from urllib.request import urlopen
from bs4 import BeautifulSoup
import sys
import re 

"""
This code scrape all data about etf from justetf.com/uk
"""

# Constant defining
URL = 'https://www.justetf.com/uk/etf-profile.html?isin={0}'

# Main dataframe
df = pd.read_csv("ISIN.txt", names=["ISIN"])
df.head(5)

# Functions defining
def scrape_etf_params(response):
    etf = {}
    def find(tag, param, value):
        try:
            return response.find(tag, {param: value}).text
        except:
            return None

    etf['name'] = find('h1', 'id', 'etf-title')
    etf['ticker'] = find('span', 'id', 'etf-second-id')

    try:
        etf_data_table_list = response.findAll('table', {'class': 'table etf-data-table'})
    except:
        etf['description'] = find('div', 'id', 'etf-description-content')
        return etf
    
    # Basics 
    basics = [
        "fund_size", "expense", "replication", "structure", "strategy", "currency", "currency_risk",
        "vol_1_year", "inception", "distribution_policy", "distribution_frequency", "domicile", "provider"
    ]
    # Performance 
    performance = [
        "YTD", "1m_return", "3m_return", "6m_return",
        "1y_return", "3y_return", "5y_return", 
        "max_return", "2022_return", "2021_return", "2020_return", "2019_return"
    ]
    # Risk
    risk = [
        "vol_1y", "vol_3y", "vol_5y", 
        "sharpe_1y", "sharpe_3y", "sharpe_5y", 
        "maxdrawdown_1y", "maxdrawdown_3y", "maxdrawdown_5y", "maxdrawdown"
    ]
    etf_info_list_of_list = [basics, performance, risk]
    for i in range(3):
        etf_info_list = etf_info_list_of_list[i]
        try:
            etf_data_table = etf_data_table_list[i].findAll('tr')
            for etf_info, etf_data in zip(etf_info_list, etf_data_table):
                try:
                    etf[etf_info] = etf_data.findAll('td')[1].text
                except:
                    etf[etf_info] = None
        except:
            for etf_info in etf_info_list:
                etf[etf_info] = None


    etf['description'] = find('div', 'id', 'etf-description-content')
    return etf

def scrape_etf(isin):
    print(isin)
    try:
        with urlopen(URL.format(isin)) as connection:
            time.sleep(3)
            response = BeautifulSoup(connection, 'html.parser')
        return scrape_etf_params(response)
    except AttributeError as e:
        print("Fund isin '{}' not found!".format(isin), file=sys.stderr)

# Main execution
result = df["ISIN"].map(scrape_etf)
df = pd.concat([df, pd.DataFrame(list(result))], axis=1)

# Saving
df.to_csv("ETF_info.csv", index=False)
df = pd.read_csv("ETF_info.csv")

# Further improvement

# Dropna
len(df)
len(df.dropna())
df = df.dropna()

# Expense to float
def convert_expense2float(expense):
    all_matches = re.findall(".*%", expense)
    return float(all_matches[0][:-1])

df.expense = df.expense.map(convert_expense2float)

# fund_size to float
def convert_fundsize2float(fund_size):
    fund_size = fund_size.replace("GBP", "")
    fund_size = fund_size.replace("m", "")
    fund_size = fund_size.replace(",", "")
    return float(fund_size)


df.fund_size = df.fund_size.map(convert_fundsize2float)

df.to_csv("ETF_info_improved.csv", index=False)
df = pd.read_csv("ETF_info_improved.csv")