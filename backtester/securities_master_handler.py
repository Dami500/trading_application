from data_handler import DataHandler
import mysql.connector as msc
import pandas as pd
from retrieve_data import get_prices_id, get_prices


class securities_master_handler(DataHandler):
    """
    This is the data handler that uses the securites master database
    to load data and support the trading system 
    """
    def __init__(self, host: string, user: string, password: string, db_name: string, symbols: list, queue: Queue):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.symbols = symbols
        self.con = None
    
    def connect(self):
        """
        connects to  secruities master database
        """
        try:
            self.con = msc.connect(host= self.host, user=self.user, password=self.password, db=self.db_name)
        except Exception as e:
            print(f"Warning! Database connection Error: {e.message}")
        return con
    
    def load(self, start_date, end_date)-> List[Dataframe]:
        """
        Loads data into a pandas dataframe
        """
        try:
            ids = get_prices_id(self.symbols, self.con)
            return get_prices(ids, self.con, start_date, end_date)
        except Exception as e:
            print(f"Error! Database Loading Error: {e.message}")
    
    def generate

    def add_market_event
            



