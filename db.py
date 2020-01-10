'''
bcutrell13@gmail.com - January 2020
'''

'''
imports
'''

import os
import csv
import code
import sqlite3
import config
import urllib.request

import pandas as pd

from io import StringIO
from datetime import datetime
from portfolios import get_prices, get_batch_prices

'''
constants
'''

FILENAME = 'data.sqlite'

CREATE_TABLES = False

SEED_DB = False
SEED_FILE = False
SEED_DB_FROM_FILE = False

USE_CONSOLE = True

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
            date TEXT NOT NULL,
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


def seed_db(c):
    find_security_sql = "SELECT id from securities WHERE ticker=(?)"
    insert_security_sql = "INSERT INTO securities (ticker) VALUES (?)"

    insert_prices_sql = "INSERT INTO prices (date, adjusted_close, security_id) VALUES (?, ?, ?)"

    for ticker in SEED_TICKERS:
        df = get_prices(ticker)

        # find or create security row
        c.execute(find_security_sql, ticker)
        security_row = c.fetchone() # returns tuple

        if not security_row:
            c.execute(insert_security_sql, ticker)
            security_id = c.lastrowid
        else:
            security_id = security_row[0]

        # create price rows
        # TODO duplicate handling
        df.index = df.index.strftime("%Y-%m-%d")
        to_db = [tuple(record) + (security_id,) for record in df.to_records()]

        c.executemany(insert_prices_sql, to_db)

def seed_db_from_file(c):
    pass


'''
main
'''

# TODO use CLI args instead of these ridiculous constants
if __name__ == "__main__":
    conn = sqlite3.connect(FILENAME)
    c = conn.cursor()

    if USE_CONSOLE:
        code.interact(local=locals())

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
