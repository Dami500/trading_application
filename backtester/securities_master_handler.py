from data_handler import DataHandler
import mysql.connector as msc
import pandas as pd
from retrieve_data import get_prices_id, get_prices
from event import market_event
import gc
import queue
import datetime
from pandas import DataFrame

class DateMismatchError(Exception):
    def __init__(self):
        self.message = """Date mismatch error, cannot Backtest 
        on the following tickers on this timeframe as data is not available for all"""
        super.__init__(self.message)

class securities_master_handler(DataHandler):
    """
    This is the data handler that uses the securites master database
    to load data and support the trading system 
    """
    def __init__(self, host: str, user: str, password: str, db_name: str, symbols: list, 
    queue: queue, start_date: datetime, end_date: datetime):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.symbols = symbols
        self.con = None
        self.start_date = start_date
        self.end_date = end_date
        self.seen_data = []
        self.total_load = None
    
    def connect(self):
        """
        connects to  secruities master database
        """
        try:
            self.con = msc.connect(host= self.host, user=self.user, password=self.password, db=self.db_name)
        except Exception as e:
            print(f"Warning! Database connection Error: {e}")
    
    def load(self)-> list[DataFrame]:
        """
        Loads data for each ticker into a list of pandas dataframes, each one being
        information for each ticker.
        """
        try:
            ids = get_prices_id(self.symbols, self.con)
            return get_prices(ids, self.con, self.start_date, self.end_date)
        except Exception as e:
            print(f"Error! Database Loading Error: {e}")
    
    def transform_into_daily_data(self)->  list[dict[str, tuple[datetime, float, float, float, float, float, float]]]:
        """
        This transforms the data into a list of dictionaries. Each one represents a new trading day.
        Each dictionary also contains multiple tickers
        For example: [{"AAPL" : [d, O, H, L, C, v], "LLY" : [O, H, L ,C]}] -> contains one day
        with each tickers date, open, high, low, close prices and volume.
        """
        daily_data = []
        dataframes  = self.load()
        for i in range(dataframes[0].shape[0]):
            # represents todays prices
            today = {}
            datecheck = dataframes[0].iloc[i, 1]
            #loops through the data creating a day "block" containing information for all the tickers on that day
            for dataframe in dataframes:
                # checks if other days align with the first date else raises an error
                if dataframe.iloc[i, 1] != datecheck:
                    raise DateMismatchError
                today[dataframe.columns.tolist()[0]] = [dataframe.iloc[i, 1], dataframe.iloc[i, 2], dataframe.iloc[i, 3],
                dataframe.iloc[i, 4], dataframe.iloc[i, 5], dataframe.iloc[i, 6]]
            daily_data.append(today)
        # delete the dataframes to free up memory
        for dataframe in dataframes:
            del dataframe
            gc.collect()
        self.total_load = daily_data
        return daily_data

    def generate_day(self, day)-> dict[str, tuple[datetime, float, float, float, float, float, float]]:
        return self.total_load[day]

    def add_market_event(self, day):
        """
        adds a new day to the queue
        """
        self.queue.put(market_event(self.total_load[day]))

import os
from dotenv import load_dotenv

load_dotenv()
db_host = os.getenv("db_host")
db_name = os.getenv("db_name")
db_pass = os.getenv("db_pass")
db_user = os.getenv("db_user")
AAPL = securities_master_handler(host = db_host, user = db_user, password = db_pass, db_name = db_name, symbols = ["AAPL"]
, queue = queue, start_date = "2010-05-12", end_date = "2020-06-13")
AAPL.connect()
AAPL.transform_into_daily_data()
print(AAPL.generate_day(1))