import mysql.connector as msc
import warnings
import pandas as pd

warnings.filterwarnings('ignore')
db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'Damilare20$'
db_name = 'securities_master'
con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name)

def get_prices_id(tickers):
    """
    Locates the corresponding symbol ID for each ticker in the list of tickers
    returns a pandas dataframe for IDs
    """
    symbols = {}
    for ticker in tickers:
        select_str = """
        SELECT securities_master.symbol.id
        from securities_master.symbol
        where securities_master.symbol.ticker = '%s'
        """ % ticker
        df = pd.read_sql_query(select_str, con)
        print(df)
        symbols[ticker] = df.iloc[0,0]
    return symbols

def get_prices(locations):
    """
    Makes use of the symbol_id list to return dataframes of the prices of those assets
    """
    dataframes = []
    for id in locations.keys():
        select_str ="""SELECT *
                       from securities_master.daily_price
                       where securities_master.daily_price.symbol_id = '%s'
                    """ % locations[id]
        data = pd.read_sql_query(select_str, con)
        specific_data = data[['symbol_id', 'price_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
        specific_data.rename(columns={'symbol_id':id}, inplace=True)
        dataframes.append(specific_data)
    for dataframe in dataframes:
        print(dataframe)

symbols = get_prices_id(['AAPL', 'GOOG', 'SPY', 'LLY'])
print(symbols)
get_prices(symbols)