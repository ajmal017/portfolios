'''
bcutrell13@gmail.com - January 2020
'''

'''
imports
'''

import os
import csv
import sqlite3
import config
import urllib.request

import pandas as pd

from io import StringIO
from datetime import datetime
from portfolios import get_prices

'''
constants
'''

FILENAME = 'data.sqlite'

CREATE_TABLES = False
SEED_DB = True
SEED_FILE = False
SEED_DB_FROM_FILE = False

SEED_FILENAME = 'prices.csv'
SEED_TICKERS = ['A']

'''
functions
'''

def create_tables(c):
    c.execute("""
        CREATE TABLE securities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL
        );
    """)

    c.execute("""
        CREATE TABLE prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id INTEGER,
            timestamp DATETIME NOT NULL,
            adjusted_close DECIMAL(12, 6) NOT NULL,
            FOREIGN KEY (security_id) REFERENCES securities (id)
        );
    """)


def seed_file():
    if os.path.exists(SEED_FILENAME):
        df = get_batch_prices(SEED_TICKERS, merge_df=pd.read_csv(SEED_FILENAME, index_col=0))
    else:
        df = get_batch_prices(SEED_TICKERS)

    df.to_csv(SEED_FILENAME)

def seed_db_from_file():
    pass

def seed_db(c):
    for ticker in tickers:
        df = get_prices(ticker)

        # create security row

        # create price rows

def get_batch_prices(tickers, merge_df=None):
    for ticker in tickers:
        df = get_prices(ticker)
        df.rename(columns={ 'adjusted_close': ticker }, inplace=True)

        if merge_df is not None:
            merge_df = merge_df.merge(df, how='outer', left_index=True, right_index=True)
        else:
            merge_df = df

    return merge_df

'''
main
'''

if __name__ == "__main__":
    conn = sqlite3.connect(FILENAME)
    c = conn.cursor()

    if CREATE_TABLES:
        create_tables(c)

    if SEED_DB:
        seed_db(c)

    if SEED_FILE:
        seed_file()

    if SEED_DB_FROM_FILE:
        seed_db_from_file(c)

    conn.commit()
    conn.close()

