import statsmodels.api as sm
from matplotlib.pyplot import scatter
from statsmodels.tsa import stattools as ts
import pandas as pd
from pandas import Timestamp
import mysql.connector as msc
import numpy as np
from numpy import cumsum, log, polyfit, sqrt, std, subtract
from numpy.random import randn
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import warnings
warnings.filterwarnings('ignore')

db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'Damilare20$'
db_name = 'securities_master'
plug ='caching_sha2_password'
con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name, auth_plugin= plug)


def obtain_data_from_sec_master(symbol_id, start_date, end_date):
    """
    return data from Sec Master into a pandas dataframe
    parse dates
    """
    select_str = """ select distinct securities_master.daily_price.price_date, 
    securities_master.daily_price.close_price 
    from securities_master.daily_price
    where securities_master.daily_price.symbol_id = %s and securities_master.daily_price.price_date >= '%s'
    and securities_master.daily_price.price_date  <= '%s'
    """ %(symbol_id, start_date, end_date)
    data = pd.read_sql_query(select_str, con, parse_dates = {"date": '%Y%m%d %H:%M:%S'})
    return data


def adf_test(data):
    """
    print out the results of the augmented dickey fuller test and change the numpy_int64 values to regular ints
    """
    results = list(ts.adfuller(data, 1))
    results[0] = float(results[0])
    results[1] = float(results[1])
    results[5] = float(results[5])
    for value in results[4]:
        results[4][value] = float(results[4][value])
    return tuple(results)


def hurst(ts):
    """Returns the Hurst Exponent of the time series vector ts"""
    # Create the range of lag values
    lags = range(2, 100)
    # Calculate the array of the variances of the lagged differences
    tau = [sqrt(std(subtract(ts[lag:], ts[:-lag]))) for lag in lags]
    # Use a linear fit to estimate the Hurst Exponent
    poly = polyfit(log(lags), log(tau), 1)
    # Return the Hurst exponent from the polyfit output
    return poly[0]*2.0


def plot_pairs_price_series(df, column_1, column_2):
    """price_series, takes a pandas DataFrame as input, with two columns
    given by the placeholder strings "ts1" and "ts2". These will be our pairs equities. The function
    simply plots the two price series on the same chart"""
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df['date'], df[column_1], label= column_1)
    ax.plot(df['date'], df[column_2], label=column_2)
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(datetime.datetime(2023, 1, 1), datetime.datetime(2024, 5, 12))
    ax.grid(True)
    fig.autofmt_xdate()
    plt.xlabel('Month / Year')
    plt.ylabel('Price($)')
    plt.title('%s and %s Daily Prices' % (column_1, column_2))
    plt.legend()
    plt.show()


def scatter_plot_pairs(df, column_1, column_2):
    plt.xlabel('%s Price($)' % column_1)
    plt.ylabel('%s Price($)' % column_2)
    plt.title('%s and %s Price Scatterplot' % (column_1, column_2))
    plt.scatter(df[column_1], df[column_2])
    plt.show()


def plot_residuals(df):
    months = mdates.MonthLocator() # every month
    fig, ax = plt.subplots()
    ax.plot(df['date'], df["res"], label="Residuals")
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(datetime.datetime(2023, 1, 1), datetime.datetime(2024, 5,12))
    ax.grid(True)
    fig.autofmt_xdate()
    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('Residual Plot')
    plt.legend()
    plt.plot(df['res'])
    plt.show()

# start_date = datetime.datetime(2011,1, 1).strftime('%Y-%m-%d')
# end_date = datetime.datetime(2013, 2, 1).strftime('%Y-%m-%d')
# data = obtain_data_from_sec_master(503,start_date, end_date)
# print(data)
# if __name__ == "__main__":
#     WMB = obtain_data_from_sec_master('10042')
#     KMI = obtain_data_from_sec_master('9827')
#     df = pd.merge(WMB, KMI, how='inner', on = "date")
#     # Plot the two time series
#     plot_pairs_price_series(df, "9827", "10042")
#     # Display a scatter plot of the two time series
#     scatter_plot_pairs(df, "9827", "10042")
#     # Calculate optimal hedge ratio "beta"
#     res = ts.OLS(endog=df['10042'], exog = sm.add_constant(df["9827"]))
#     beta = res.fit()
#     beta_hr = beta.params.iloc[1]
#     # Calculate the residuals of the linear combination
#     df["res"] = df["10042"] - beta_hr * df["9827"]
#     print(df)
#     # Plot the residuals
#     plot_residuals(df)
#     # Calculate and output the CADF test on the residuals
#     cadf = adf_test(df['res'])
#     print(cadf)
