import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

data = pd.read_parquet(f'data/parquet/PROCESSED_TABLE_METADATA.parquet')
data['Execution time (s) / row'] = data['Execution time'] / data['Row count ']
data['Peak memory usage [MB] / row'] = data['Peak memory usage'] / data['Row count ']
data = data.sort_values(by=['Execution time (s) / row'])

fig, ax = plt.subplots(nrows=2, ncols=2)
fig.set_figwidth(12)
fig.set_figheight(8)
sns.barplot(data, x='Execution time (s) / row', y='File mode', ax=ax[0][0])
sns.barplot(data, x='Execution time (s) / row', y='Processing mode', ax=ax[0][1])

sns.barplot(data, x='Peak memory usage [MB] / row', y='File mode', ax=ax[1][0])
sns.barplot(data, x='Peak memory usage [MB] / row', y='Processing mode', ax=ax[1][1])

plt.show()


data = pd.read_parquet(f'data/parquet/PROCESSED_COLUMN_NULLNESS.parquet')
data['Completeness [%]'] = 100 - data['Nullness']

data = data.sort_values(by=['Completeness [%]'])
plt.figure(figsize=(16, 9), dpi=80)

sns.barplot(data[data['Nullness'] > 0], x='Completeness [%]', y='Column Name', hue='Table Name')
plt.yticks(rotation=40)
plt.xlim((0, 100))
plt.legend(fontsize="20")
plt.xlabel(xlabel='Completeness [%]', fontsize=20)
plt.xticks(fontsize="16")

plt.show()

data = pd.read_parquet(f'data/parquet/PROCESSED_COUNT_BY_DATE.parquet')
data['Year'] = data['Date'].dt.strftime('%Y').astype(int)

sns.barplot(data[data['Year'] >= 2013], x='Year', y='Count', hue='Table Name')
plt.yscale('log')
plt.show()