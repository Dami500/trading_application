These are the Python Scripts I use for setting up my virtual environment

Symbols.py obtains the tickers for the companies on the S&P 500 as well as their sector and currency and stores it on an SQL database

Price retrieval.py collects pricing data from the Yahoo finance API and stores it on a table in an SQL database. I find it easier to keep all pricing data stored there

I download CSV files containing pricing data for futures contracts from the nasdaqdatalink. futures.py reads this data into a dataframe and visualizes it using seaborn 

Retrieve data.py is a test page that collects data from my SQL database into a dataframe. 

Time_series_tests performs statistical tests on data. These tests include: Augmented dickey fuller test, Hurst exponent as well as cointegrated augmented dickey fuller test.

Detect_cointegrated_mean_reversion.py performs the cointegrated augmented dickey fuller test on every possible combination of companies in the same sector. It was written to help detect which companies I can use mean reversion for pairs trading.

I currently working on sequential learning techniques to develop models for trading using interactive brokers. 
