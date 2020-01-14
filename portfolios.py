'''
bcutrell13@gmail.com - January 2020
'''

'''
imports
'''

import os
import urllib.request
import zipfile
import json
import time

import numpy as np
import pandas as pd
import statsmodels.api as smf

from io import StringIO
from datetime import datetime
from scipy import stats

import config

'''
constants
'''

DATA_DIR = 'data'
FAMA_FRENCH_URL = 'https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip'
FAMA_FRENCH_ZIP = DATA_DIR + '/fama_french.zip'
FAMA_FRENCH_CSV = DATA_DIR + '/F-F_Research_Data_Factors.csv'

ALPHAVANTAGE_URL = 'https://www.alphavantage.co/query?'

ALPHAVANTAGE_CALL_LIMIT_NOTE = 'Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute and 500 calls per day. Please visit https://www.alphavantage.co/premium/ if you would like to target a higher API call frequency.'

'''
objects
'''

class AlphavantageCallLimitException(Exception):
    pass

class AlphavantageException(Exception):
    pass

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

def _get_prices(ticker):
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'datatype': 'csv',
        'outputsize': 'full',
        'symbol': ticker,
        'apikey': config.ALPHAVANTAGE_API_KEY
    }

    src_url = ALPHAVANTAGE_URL + urllib.parse.urlencode(params)
    resp = urllib.request.urlopen(src_url)
    resp_text = resp.read().decode("utf-8")

    try:
        df = pd.read_csv(StringIO(resp_text), usecols=['adjusted_close', 'timestamp'], index_col=0)
        df.index = pd.to_datetime(df.index)
    except:
        if json.loads(resp_text)['Note'] == ALPHAVANTAGE_CALL_LIMIT_NOTE:
            raise AlphavantageCallLimitException

    return df

def get_prices(ticker):
    """
    Get prices from alphavantage API.
    For now, only handle the per minute call limit error
    """

    try:
        return _get_prices(ticker)
    except AlphavantageCallLimitException:
        print("Call frequency exceeded, sleeping for 1 minute")
        time.sleep(60) # wait 1 minute
        return get_prices(ticker)
    except Exception as err:
        print("Failed to get prices from alphavantage: {}".format(err))

def get_batch_prices(tickers, merge_df=None):
    for ticker in tickers:
        df = get_prices(ticker)
        df.rename(columns={ 'adjusted_close': ticker }, inplace=True)

        if merge_df is not None:
            merge_df = merge_df.merge(df, how='outer', left_index=True, right_index=True)
        else:
            merge_df = df

    return merge_df

def get_returns(prices, period="M"):
    resampled_prices = prices.resample(period).last()

    returns = resampled_prices.pct_change()[1:]
    returns = pd.DataFrame(returns)

    returns.columns = ['portfolio']

    return returns

def get_beta(prices):
    prices = prices.dropna()
    port_ret = get_returns(prices)

    benchmark = get_prices('IVV')
    benchmark_ret = get_returns(benchmark)

    # make sure both return series are using the same dates
    last_date = max(port_ret.index.min(), benchmark_ret.index.min())
    port_ret = port_ret[last_date:]
    benchmark_ret = benchmark_ret[last_date:]

    model = stats.linregress(benchmark_ret.values.flatten(), port_ret.values.flatten())

    (beta, alpha) = [round(val, 2) for val in model[0:2]]
    print("Beta: {} Alpha: {}".format(beta, alpha))

    return model

def run_factor_regression(prices, periods=60):
    factors = get_fama_french()
    factor_last = factors.index[factors.shape[0] - 1].date()

    prices = prices.loc[:factor_last]

    returns = get_returns(prices)
    returns = returns.tail(periods)

    all_data = pd.merge(returns, factors, how='inner', left_index=True, right_index=True)
    all_data.rename(columns={ "Mkt-RF": "mkt_excess" }, inplace=True)
    all_data['port_excess'] = all_data['portfolio'] - all_data['RF']

    model = smf.formula.ols(formula = "port_excess ~ mkt_excess + SMB + HML", data=all_data).fit()
    print(model.params)

    return model

def get_cov_matrix(prices, periods=252):
    log_ret = np.log(prices/prices.shift(1))
    return log_ret.cov() * periods # annualized by default

def run_monte_carlo_optimization(prices, simulations=5000):
    log_ret = np.log(prices/prices.shift(1))
    cov_mat = log_ret.cov() * 252
    print(cov_mat)

    # Creating an empty array to store portfolio weights, returns, risks, and sharpe ratio
    all_wts = np.zeros((simulations, len(prices.columns)))
    port_returns = np.zeros((simulations))
    port_risk = np.zeros((simulations))
    sharpe_ratio = np.zeros((simulations))

    for i in range(simulations):
      wts = np.random.uniform(size = len(prices.columns))
      wts = wts/np.sum(wts)
      all_wts[i,:] = wts

      # Portfolio Returns
      port_ret = np.sum(log_ret.mean() * wts)
      port_ret = (port_ret + 1) ** 252 - 1

      # Saving Portfolio returns
      port_returns[i] = port_ret

      # Portfolio Risk
      port_sd = np.sqrt(np.dot(wts.T, np.dot(cov_mat, wts)))
      port_risk[i] = port_sd

      # Portfolio Sharpe Ratio
      # Assuming 0% Risk Free Rate
      sr = port_ret / port_sd
      sharpe_ratio[i] = sr

    names = prices.columns
    min_var = all_wts[port_risk.argmin()]
    print("Minimum Variance Portfolio")
    print(names)
    print(min_var)

    print("")

    max_sr = all_wts[sharpe_ratio.argmax()]
    print("Maximum Sharpe Portfolio")
    print(names)
    print(max_sr)

def run_sharpe_optimization():
    pass


'''
main
'''

if __name__ == '__main__':

    tickers = ['AMZN', 'NFLX']
    prices = get_batch_prices(tickers)

    run_factor_regression(prices['AMZN'])
    get_beta(prices['NFLX'])
    run_monte_carlo_optimization(prices)

