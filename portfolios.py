'''
bcutrell13@gmail.com - January 2020
'''

'''
imports
'''

import os
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime

import statsmodels.api as smf
import urllib.request
import zipfile

import config

'''
objects
'''

class Portfolio(object):
    '''
    allocation = {
        'IVV': 0.6,
        'TLT': 0.2,
        'IAU': 0.2
    }
    portoflio = Portfolio(allocation)
    '''

    def __init__(self, current):
        self.current = current
        self.tickers = current.keys()

'''
constants
'''

DATA_DIR = 'data'
FAMA_FRENCH_URL = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip'
FAMA_FRENCH_ZIP = DATA_DIR + '/fama_french.zip'
FAMA_FRENCH_CSV = DATA_DIR + '/F-F_Research_Data_Factors.csv'

ALPHAVANTAGE_URL = 'https://www.alphavantage.co/query?'

'''
functions
'''

def get_fama_french():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    if not os.path.exists(FAMA_FRENCH_CSV):
        urllib.request.urlretrieve(FAMA_FRENCH_URL, FAMA_FRENCH_ZIP)

        zip_file = zipfile.ZipFile(FAMA_FRENCH_ZIP, 'r')
        zip_file.extractall(DATA_DIR)
        zip_file.close()

    # read the csv twice to account for null rows
    factors = pd.read_csv(FAMA_FRENCH_CSV, skiprows=3, index_col=0)
    ff_row = factors.isnull().any(1).to_numpy().nonzero()[0][0]
    factors = pd.read_csv(FAMA_FRENCH_CSV, skiprows=3, nrows=ff_row, index_col=0)

    # set date index to end of month
    factors.index = pd.to_datetime(factors.index, format='%Y%m')
    factors.index = factors.index + pd.offsets.MonthEnd()

    factors = factors.apply(lambda x: x/ 100)

    return factors

def get_prices(ticker):
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'datatype': 'csv',
        'outputsize': 'full',
        'symbol': ticker,
        'apikey': config.ALPHAVANTAGE_API_KEY
    }

    src_url = ALPHAVANTAGE_URL + urllib.parse.urlencode(params)
    resp_text = urllib.request.urlopen(src_url).read().decode("utf-8")

    df = pd.read_csv(StringIO(resp_text), usecols=['adjusted_close', 'timestamp'], index_col=0)
    df.index = pd.to_datetime(df.index)

    return df

def get_returns(price_data, period="M"):
    price = price_data.resample(period).last()

    ret_data = price.pct_change()[1:]
    ret_data = pd.DataFrame(ret_data)

    ret_data.columns = ['portfolio']

    return ret_data

def run_factor_regression(ticker, periods=60):
    factors = get_fama_french()
    factor_last = factors.index[factors.shape[0] - 1].date()

    prices = get_prices(ticker)
    prices = prices.loc[factor_last:]

    returns = get_returns(prices)
    returns = returns.tail(periods)

    all_data = pd.merge(returns, factors, how='inner', left_index=True, right_index=True)
    all_data.rename(columns={ "Mkt-RF": "mkt_excess" }, inplace=True)
    all_data['port_excess'] = all_data['portfolio'] - all_data['RF']

    model = smf.formula.ols(formula = "port_excess ~ mkt_excess + SMB + HML", data=all_data).fit()
    return model.params

'''
main
'''

if __name__ == '__main__':
    print(run_factor_regression('VTV'))

