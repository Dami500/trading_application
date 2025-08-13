import pandas as pd
import mysql.connector as msc
import numpy as np
from statsmodels.tsa import stattools as ts
import statsmodels.api as sm
import time_series_tests as tst
from time_series_tests import adf_test
import warnings

db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'Password'
db_name = 'securities_master'
con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name)


def collect_sectors():
    """
    collect_sectors() gathers all the different sectors occupied by companies in the S&P500
    and returns a list. It collects the data from an SQL database
    """
    select_str = """select distinct securities_master.‘symbol‘.‘sector‘
                    from securities_master.‘symbol‘ 
                    """
    sector_list = []
    data = pd.read_sql(select_str, con)
    for i in data.iloc[:, 0]:
        sector_list.append(i)
    return sector_list


def sector_symbol_id(sectors):
    """
    this function takes a list of sectors and returns a dictionary with the sectors as the keys and
    the symbol_ids as the values. It collects the data from an SQL database
    """
    sector_symbol_dict = {}
    for sector in sectors:
        select_str = """select securities_master.‘symbol‘.‘id‘
                        from securities_master.‘symbol‘
                        where securities_master.‘symbol‘.‘sector‘ = '%s'
                        """ % sector
        data = pd.read_sql(select_str, con)
        sector_symbol_dict[sector] = []
        for i in data.iloc[:, 0]:
            sector_symbol_dict[sector].append(i)
    return sector_symbol_dict


def iterate_adf(sector_symbol_dict):
    """
    this function takes the sector_symbol_dict and performs the adf test on all paired combinations
    of the companies in the same sector. It returns a list of pairs of companies that satisfy the co-integrated
    dickey-fuller test
    """
    mean_reversion_list = []
    for sector in sector_symbol_dict:
        for i in range(0, len(sector_symbol_dict[sector])):
            j = 1
            while (i + j) < len(sector_symbol_dict[sector]):
                select_str = """select securities_master.‘daily_price‘.‘price_date‘ as date, 
                                securities_master.‘daily_price‘.‘close_price‘ as close_price‘
                                from securities_master.‘daily_price‘
                                where securities_master.‘daily_price‘.‘symbol_id‘ = %s
                                """ % sector_symbol_dict[sector][i]
                data_1 = pd.read_sql(select_str, con)
                select_str = """select securities_master.‘daily_price‘.‘price_date‘ as date, 
                                securities_master.‘daily_price‘.‘close_price‘ as close_price
                                from securities_master.‘daily_price‘
                                where securities_master.‘daily_price‘.‘symbol_id‘ = %s
                                """ % sector_symbol_dict[sector][i+j]
                data_2 = pd.read_sql(select_str, con)
                df = pd.merge(data_1, data_2, how='inner', on="date")
                res = ts.OLS(endog=df.iloc[:,1], exog=sm.add_constant(df.iloc[:, 2]))
                beta = res.fit()
                beta_hr = beta.params.iloc[1]
                df["res"] = df.iloc[:, 1] - beta_hr * df.iloc[:, 2]
                cadf = tst.adf_test(df['res'])
                for item in cadf[4]:
                    if cadf[1] < cadf[4][item]:
                        mean_reversion_list.append('sector_symbol_dict[sector][i]-sector_symbol_dict[sector][i+j]')
                j += 1
    return mean_reversion_list


if __name__ == '__main__':
    warnings.filterwarnings('ignore')
    # collect sectors and assign to a variable
    sectors = collect_sectors()
    print(sectors)
    # make sector-symbol dictionary
    sector_symbol_dict = sector_symbol_id(sectors)
    print(sector_symbol_dict)
    # perform cadf test integrations
    mean_reversion_list = iterate_adf(sector_symbol_dict)
    print(mean_reversion_list)
