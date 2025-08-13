import datetime
from datetime import datetime
import numpy as np
import pandas as pd
import mysql.connector as msc
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis as LDA
from sklearn.metrics import confusion_matrix
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis as QDA
from sklearn.svm import LinearSVC, SVC
import warnings

warnings.filterwarnings('ignore')
db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'Password'
db_name = 'securities_master'
con = msc.connect(host=db_host, user=db_user, password=db_pass, db=db_name)


def obtain_lagged_series(symbol, start_date, end_date, lags = 5):
    """
    This creates a Pandas DataFrame that stores the percentage returns of the adjusted closing value of
    a stock obtained from Yahoo Finance, along with a number of lagged returns from the prior trading days
    (lags defaults to 5 days). Trading volume, as well as the Direction from the previous day, are also included
    """
    symbol_select_str = """SELECT securities_master.‘symbol‘.‘id‘
                    FROM securities_master.‘symbol‘
                    where securities_master.‘symbol‘.‘ticker‘ = '%s' """ % symbol
    df = pd.read_sql_query(symbol_select_str, con)
    symbol_id = df.iloc[0, 0]
    f_start_date = start_date.strftime('%Y-%m-%d')
    f_end_date = end_date.strftime('%Y-%m-%d')
    price_select_str = """SELECT distinct securities_master.‘daily_price‘.close_price,
                          securities_master.‘daily_price‘.volume
                          FROM securities_master.‘daily_price‘
                          where securities_master.‘daily_price‘.symbol_id = '%d' and
                          securities_master.‘daily_price‘.price_date >= '%s' and 
                          securities_master.‘daily_price‘.price_date <= '%s'
                          """ % (symbol_id, f_start_date, f_end_date)
    df = pd.read_sql_query(price_select_str, con)
    # Create the new lagged DataFrame
    df_lag = pd.DataFrame(index=df.index)
    df_lag['today'] = df['close_price']
    df_lag['volume'] = df['volume']
    # Create the shifted lag series of prior trading period close values
    for i in range(lags):
        df_lag['lag%s' % str(i+1)] = df_lag['today'].shift(i+1)
    # Create the returns DataFrame
    df_returns = pd.DataFrame(index=df_lag.index)
    df_returns['volume'] = df_lag['volume']
    df_returns['percent_change'] = df_lag['today'].pct_change()*100
    df_returns['percent_change'] = df_returns['percent_change'].fillna(0)
    # If any of the values of percentage returns equal zero, set them to
    # a small number (stops issues with QDA model in Scikit-Learn)
    for i, x in enumerate(df_returns["percent_change"]):
        if abs(x) < 0.0001:
            df_returns['percent_change'][i] = 0.0001
    # Create the lagged percentage returns columns
    for i in range(lags):
        df_returns["lag%s" % str(i + 1)] = df_lag["lag%s" % str(i + 1)].pct_change() * 100.0
    # Create the "Direction" column (+1 or -1) indicating an up/down day
    df_returns["direction"] = np.sign(df_returns["percent_change"])
    return df_returns.fillna(0)


if __name__ == '__main__':
    # create a lagged series of returns for any stock on the S&P
    stock_returns = obtain_lagged_series('LLY', datetime(2019,1,1),
                                         datetime(2024, 8, 20), 2)
    # print(stock_returns)
    X = stock_returns[["lag1", "lag2"]]
    y = stock_returns["direction"]
    # split the data into training data and validation data
    train_x = X.iloc[:750]
    train_y = y.iloc[:750]
    val_x = X.iloc[750:]
    val_y = y.iloc[750:]
    # Create the (parametrised) models you will be using
    models = [("LR", LogisticRegression()),
              ("LDA", LDA()),
              ("QDA", QDA()),
              ("LSVC", LinearSVC()),
              ("RSVM", SVC(
                  C=1000000.0, cache_size=200, class_weight=None,
                  coef0=0.0, degree=3, gamma=0.0001, kernel='rbf',
               max_iter=-1, probability=False, random_state=None,
               shrinking=True, tol=0.001, verbose=False)),
              ("RF", RandomForestRegressor(
                n_estimators=1000, criterion='absolute_error',
                max_depth=None, min_samples_split=2,
                min_samples_leaf=1, max_features= 5000,
                bootstrap=True, oob_score=False, n_jobs=1,
                random_state=None, verbose=0))]
    # Iterate through the models
    for model in models:
        model[1].fit(train_x, train_y)
        # Make an array of predictions on the value set
        predictions = model[1].predict(val_x)
        # Output the hit-rate and confusion matrix for each model
        print('using %s your hit_rate is %s' % (model[0], model[1].score(val_x, val_y)))
        print('using %s your confusion matrix is %s' % (model[0], confusion_matrix(val_y, predictions)))
