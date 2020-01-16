'''
bcutrell13@gmail.com - January 2020

Examples:
    python3 db.py seed_file --tickers tickers.csv
    python3 db.py seed_file --tickers IVV
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

import argparse

'''
constants
'''

FILENAME = 'data.sqlite'
SEED_FILENAME = 'prices.csv'
SEED_TICKERS = ['A']

CREATE_TABLES = 'create_tables'
SEED_DB = 'seed_db'
SEED_FILE = 'seed_file'
USE_CONSOLE = 'use_console'

COMMANDS = [CREATE_TABLES, SEED_DB, SEED_FILE, USE_CONSOLE]

'''
objects
'''

class DummyDB(object):
    pass

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


def seed_file(filename, tickers):
    if os.path.exists(filename):
        df = get_batch_prices(tickers, merge_df=pd.read_csv(filename, index_col=0))
    else:
        df = get_batch_prices(tickers)

    df.to_csv(filename)


def seed_db(c, tickers):
    find_security_sql = "SELECT id from securities WHERE ticker=(?)"
    insert_security_sql = "INSERT INTO securities (ticker) VALUES (?)"

    insert_prices_sql = "INSERT OR REPLACE INTO prices (date, adjusted_close, security_id) VALUES (?, ?, ?)"

    for ticker in tickers:
        df = get_prices(ticker)

        # find or create security row
        c.execute(find_security_sql, [ticker])
        security_row = c.fetchone() # returns tuple

        if not security_row:
            c.execute(insert_security_sql, [ticker])
            security_id = c.lastrowid
        else:
            security_id = security_row[0]

        # create price rows
        df.index = df.index.strftime("%Y-%m-%d")
        to_db = [tuple(record) + (security_id,) for record in df.to_records()]

        c.executemany(insert_prices_sql, to_db)

def get_tickers(tickers):
    if not tickers:
        print("Missing tickers argument")
        exit()

    try:
        if os.path.exists(tickers):
            with open(tickers) as f:
                tickers = f.readlines()
                tickers = [ticker.strip() for ticker in tickers]
        else:
            tickers = tickers.split(',')
    except:
        print("Invalid tickers {}".format(tickers))
        exit()

    return tickers


'''
main
'''

if __name__ == "__main__":
    my_parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    my_parser.add_argument('command', help='One of {}'.format(', '.join(COMMANDS)))

    my_parser.add_argument('--tickers', help='A list of tickers to get prices for. Can be file containing a list of tickers seperated by a newline or a comma seperated list with no spaces e.g. AAPL,TSLA,JPM')

    args = my_parser.parse_args()

    command = args.command

    if command not in COMMANDS:
        print("Invalid command {}".format(command))
        exit()

    '''
    non-db commands
    '''

    if command == SEED_FILE:
        tickers = get_tickers(args.tickers)
        seed_file(SEED_FILENAME, tickers)
        exit()

    '''
    db commands
    '''

    # Start DB
    conn = sqlite3.connect(FILENAME)
    c = conn.cursor()

    if command == USE_CONSOLE:
        code.interact(local=locals())

    elif command == CREATE_TABLES:
        create_tables(c)

    elif command == SEED_DB:
        tickers = get_tickers(args.tickers)
        seed_db(c, tickers)

    # Stop DB
    conn.commit()
    conn.close()

