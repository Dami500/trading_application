from __future__ import print_function
import datetime
from math import ceil
import bs4
import mysql.connector as msc
import requests
# Connect to the MySQL instance
import os
from dotenv import load_dotenv

load_dotenv()
db_host = os.getenv("db_host")
db_name = os.getenv("db_name")
db_pass = os.getenv("db_pass")
db_user = os.getenv("db_user")


con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name)


def obtain_parse_wiki_snp500():
    """
    Download and parse the Wikipedia list of S&P500 constituents using requests and BeautifulSoup.
    Returns a list of tuples for to add to MySQL.
     """
    # Stores the current time, for the created_at record
    now = datetime.datetime.now()
    # Use requests and BeautifulSoup to download the list of S&P500 companies and obtain the symbol table
    # list of S&P500 companies and obtain the symbol table
    response = requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    soup = bs4.BeautifulSoup(response.text, features="lxml")
    # This selects the first table, using CSS Selector syntax and then ignores the header row ([1:])
    symbols_list = soup.select('table')[0].select('tr')[1:]
    # Obtain the symbol information for each row in the S&P500 constituent table
    sign = []
    for i, symbol in enumerate(symbols_list):
        tds = symbol.select('td')
        sign.append(
            (
                tds[0].select('a')[0].text,       # Ticker
                'stock',
                tds[1].select('a')[0].text,  # Name
                tds[3].text,         # sector
                'USD', now, now
            )
        )
    return sign


def insert_snp500_symbols(sign):
    """
    Insert the S&P500 symbols into the MySQL database.
    """
    # Create the insert strings
    insert_str = """ INSERT INTO securities_master.symbol (securities_master.symbol.ticker,
    securities_master.symbol.instrument,securities_master.symbol.name, securities_master.symbol.sector,
    securities_master.symbol.currency,securities_master.symbol.created_date,
    securities_master.symbol.last_updated_date) VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    # Using the MySQL connection, carry out
    # an INSERT INTO for every symbol
    with con:
        cur = con.cursor()
        cur.executemany(insert_str, sign[1:503])
        con.commit()


if __name__ == "__main__":
    sign = obtain_parse_wiki_snp500()
    print(sign)
    insert_snp500_symbols(sign[0:503])
    print("%s symbols were successfully added." % len(sign))
