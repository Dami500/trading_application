from data_handler import DataHandler
import mysql.connector as msc
import pandas as pd
from retrieve_data import get_prices_id, get_prices
from event import market_event
import gc

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
    def __init__(self, host: string, user: string, password: string, db_name: string, symbols: list, 
    queue: Queue, start_date: datetime, end_date: datetime):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.symbols = symbols
        self.con = None
        self.start_date = start_date
        self.end_date = end_date
        self.seen_data = []
        self.total_load = self.transform_into_daily_data()
    
    def connect(self):
        """
        connects to  secruities master database
        """
        try:
            self.con = msc.connect(host= self.host, user=self.user, password=self.password, db=self.db_name)
        except Exception as e:
            print(f"Warning! Database connection Error: {e.message}")
        return con
    
    def load(self, start_date: datetime, end_date: datetime)-> List[Dataframe]:
        """
        Loads data for each ticker into a list of pandas dataframes, each one being
        information for each ticker.
        """
        try:
            ids = get_prices_id(self.symbols, self.con)
            return get_prices(ids, self.con, start_date, end_date)
        except Exception as e:
            print(f"Error! Database Loading Error: {e.message}")
    
    def transform_into_daily_data(self)-> List[Dict[str[datetime, float, float, float, float, float, float]]]:
        """
        This transforms the data into a list of dictionaries. Each one represents a new trading day.
        Each dictionary also contains multiple tickers
        For example: [{"AAPL" : [d, O, H, L, C, v], "LLY" : [O, H, L ,C]}] -> contains one day
        with each tickers date, open, high, low, close prices and volume.
        """
        daily_data = []
        dataframes  = self.load(self.start_date, self.end_date)
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
        
        return today

    def generate_day(self, day)-> Dict[str[datetime, float, float, float, float, float, float]]:
        return self.total_load[day]

    def add_market_event(self, i):
        """
        adds a new day to the queue
        """
        self.Queue.put(market_event(self.total_load[i]))



