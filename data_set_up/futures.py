from __future__ import print_function
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


historical_data = pd.read_csv('/home/damilare-ajayi-obe/Downloads/HistoricalData_1720804976799.csv',
                              index_col = "Date", parse_dates = True)
plt.figure(figsize=(20,60))
sns.lineplot(data = historical_data['Close/Last'])
sns.lineplot(data = historical_data['Low'])
plt.show()
print(historical_data)




