from __future__ import print_function
import datetime
import warnings
import mysql.connector as msc
import yfinance as yf
import numpy
from pandas import Timestamp
# Obtain a database connection to the MySQL instance

import os
from dotenv import load_dotenv

load_dotenv()
db_host = os.getenv("db_host")
db_name = os.getenv("db_name")
db_pass = os.getenv("db_pass")
db_user = os.getenv("db_user")


con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name)


def obtain_list_of_db_tickers():
    """
    Obtains a list of the ticker symbols in the database.
    """
    con = msc.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name, connect_timeout=28800)
    with con:
        cur = con.cursor()
        cur.execute("""SELECT securities_master.symbol.id, securities_master.symbol.ticker
        FROM securities_master.symbol """)
        data = cur.fetchall()
        db_tick = []
        for d in data:
            tup = (d[0], d[1])
            db_tick.append(tup)
        return db_tick


def convert_numpy_int_to_int(obj):
    for x in range(len(obj)):
        for y in range(len(obj[0])):
            if isinstance(obj[x][y], numpy.int64):
                obj[x] = list(obj[x])
                obj[x][y] = int(obj[x][y])
                obj[x] = tuple(obj[x])
            else:
                pass
    return obj


def get_daily_historic_data_yahoo(ticker, start_date=datetime.datetime(2001, 1, 1), end_date=datetime.date.today()):
    """
    Obtains data from Yahoo Finance returns and a list of tuples.
    ticker: Yahoo Finance ticker symbol, e.g. "GOOG" for Google, Inc.
    start_date: Start date in (YYYY, M, D) format
    end_date: End date in (YYYY, M, D) format
    """
    # Create ticker object
    ticker = yf.Ticker(ticker)
    h_df = ticker.history(start=start_date, end=end_date, interval='1d')
    h_df.reset_index(inplace = True)
    prices = []
    for p in range(h_df.shape[0]):
        tup = (h_df.iloc[p, 0].to_pydatetime(), float(h_df.iloc[p, 1]), float(h_df.iloc[p, 2]),
               float(h_df.iloc[p, 3]), float(h_df.iloc[p, 4]), float(h_df.iloc[p, 5]),
               float(h_df.iloc[p, 6]), float(h_df.iloc[p, 7]))
        prices.append(tup)
    return prices


def convert_to_daily_data(data_vendor_id, symbol_id, data):
    now = datetime.datetime.now()
    records = []
    for tup in data:
        new_tup = (data_vendor_id, symbol_id, tup[0], now, now, tup[1], tup[2], tup[3], tup[4], tup[5], tup[6], tup[7])
        records.append(new_tup)
    return records


def update_the_pricing_data(data_vendor_id, symbol_id, data):
    now = datetime.datetime.now()
    records = []
    for tup in data:
        if tup[0]>= Timestamp('2025-05-16 00:00:00', tz='America/New_York'):
            new = (data_vendor_id, symbol_id, tup[0], now, now, tup[1], tup[2], tup[3], tup[4], tup[5], tup[6], tup[7])
            records.append(new)
    return records


def insert_daily_data_into_db(daily_data):
    """
    Takes a list of tuples of daily data and adds it to the
    MySQL database. Appends the vendor ID and symbol ID to the data.
    daily_data: List of tuples of the OHLC data (with
    adj_close and volume)
    """
    # Create the insert strings
    insert_str = """ INSERT INTO securities_master.daily_price
    (securities_master.daily_price.data_vendor_id, securities_master.daily_price.symbol_id,
    securities_master.daily_price.price_date, securities_master.daily_price.created_date,
    securities_master.daily_price.last_updated_date, securities_master.daily_price.open_price,
    securities_master.daily_price.high_price, securities_master.daily_price.low_price, 
    securities_master.daily_price.close_price, securities_master.daily_price.volume, 
    securities_master.daily_price.dividends, securities_master.daily_price.stock_splits) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                                                                                     %s, %s, %s, %s)
    """
    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    con = msc.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name, connect_timeout=28800)
    with con:
        cur = con.cursor()
        cur.executemany(insert_str, daily_data)
        con.commit()


if __name__ == "__main__":
    # This ignores the warnings regarding Data Truncation
    # from the Yahoo precision to Decimal(19,4) datatypes
    warnings.filterwarnings('ignore')
    # Loop over the tickers and insert the daily historical
    # data into the database
    tickers = obtain_list_of_db_tickers()
    len_tickers = len(tickers)
    for i, t in enumerate(tickers):
        print("Adding data for %s: %s out of %s" % (t[1], i+1, len_tickers))
        yf_data = get_daily_historic_data_yahoo(t[1])
        dyf_data = convert_numpy_int_to_int(yf_data)
        # new_data = convert_to_daily_data('1', t[0], dyf_data)
        new_data = update_the_pricing_data('1', t[0], dyf_data)
        # print(new_data[0])
        # print(len(new_data[0]))
        # break
        insert_daily_data_into_db(new_data)
    print("Successfully added Yahoo Finance pricing data to DB.")
# aapl= get_daily_historic_data_yahoo('AAPL', start_date=datetime.datetime(2001, 1, 1), end_date=datetime.date.today())
# print(convert_to_daily_data(1, 1, aapl)[0])