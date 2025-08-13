import yfinance as yf
import pandas as pd


def get_fundamentals_dataframe(tick, symbol_id):
    """Obtains all the fundamental data we care about into a single dataframe
    for iteration into our database
    """
    ticker_object = yf.Ticker(tick)
    df1 = ticker_object.quarterly_financials.loc[['Basic EPS','Diluted EPS','Net Income', 'EBIT', 'EBITDA', 'Gross Profit', 'Operating Revenue', 'Total Revenue']]
    df1 = df1.T.reset_index()
    df2 = ticker_object.quarterly_balance_sheet.loc[['Total Debt', 'Stockholders Equity', 'Current Assets', 'Current Liabilities', 'Tangible Book Value']]
    df2 = df2.T.reset_index()
    final_df = pd.merge(df1, df2, how = 'inner', on = 'index')
    final_df['symbol_id'] = [symbol_id for i in range(0, len(final_df))]
    final_df['market_cap'] = [ticker_object.info.get('marketCap') for i in range(0, len(final_df))]
    return final_df.fillna(0)











