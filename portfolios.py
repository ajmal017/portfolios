'''
bcutrell13@gmail.com - January 2020
'''

'''
imports
'''

import os
import urllib.request
import zipfile

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
    prices = prices.resample(period).last()

    returns = prices.pct_change()[1:]
    returns = pd.DataFrame(returns)

    returns.columns = ['portfolio']

    return returns

def get_beta(ticker):
    prices = get_prices(ticker)
    port_ret = get_returns(prices)

    benchmark = get_prices('IVV')
    benchmark_ret = get_returns(benchmark)

    # make sure both return series are using the same dates
    last_date = max(port_ret.index.min(), benchmark_ret.index.min())
    port_ret = port_ret[last_date:]
    benchmark_ret = benchmark_ret[last_date:]

    model = stats.linregress(benchmark_ret.values.flatten(), port_ret.values.flatten())

    (beta, alpha) = model[0:2]
    print("Beta: {} Alpha: {}".format(beta, alpha))

    return model

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
    print(model.params)

    return model

def get_cov_matrix(prices, periods=252):
    log_ret = np.log(prices/prices.shift(1))
    return log_ret.cov() * periods # annualized by default

def run_monte_carlo_optimization(prices, simulations=5000):
    cov_mat = get_cov_matrix(prices)
    print(cov_mat)

    # Creating an empty array to store portfolio weights
    all_wts = np.zeros((simulations, len(price_data.columns)))

    # Creating an empty array to store portfolio returns
    port_returns = np.zeros((simulations))

    # Creating an empty array to store portfolio risks
    port_risk = np.zeros((simulations))

    # Creating an empty array to store portfolio sharpe ratio
    sharpe_ratio = np.zeros((simulations))

    for i in range(simulations):
      wts = np.random.uniform(size = len(price_data.columns))
      wts = wts/np.sum(wts)

      # saving weights in the array
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

    names = price_data.columns
    min_var = all_wts[port_risk.argmin()]
    print(names)
    print(min_var)

    max_sr = all_wts[sharpe_ratio.argmax()]
    print(names)
    print(max_sr)


'''
main
'''

if __name__ == '__main__':

    tickers = ['AMZN', 'AAPL', 'NFLX', 'XOM', 'T']
    prices = get_batch_prices(tickers)

    run_factor_regression(prices['AMZN'])
    get_beta(prices['TSLA'])
    run_monte_carlo_optimization(tickers)

