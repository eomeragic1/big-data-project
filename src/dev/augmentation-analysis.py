import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from box import Box
from dask import dataframe as dd
from dask.distributed import Client

from util.custom.common import get_dask_cluster

if __name__ == '__main__':
    config = Box.from_yaml(filename='config/config.yaml')
    cluster = get_dask_cluster(config=config,
                               environment_name='local')

    with Client(cluster,
                timeout='120s') as client:
        data = dd.read_parquet('data/parquet/AUGMENTED_PARKING_VIOLATION_ISSUED.parquet')
        data_filtered = data[['PE - Date', 'PE - Event Borough', 'PE - Events Count',
                              'S - Police Precinct', 'S - Number of Schools', 'W - Date', 'W - Time',
                              'W - Temperature', 'W - Precipitation', 'W - Rain', 'W - Snowfall',
                              'W - Borough']]
        len_data = len(data_filtered)
        data_nullness = pd.DataFrame(
            [{col: 100 * (data_filtered[col].isnull().sum().compute()) / len_data for col in data_filtered.columns}]
        ).T.reset_index()
        data_nullness.columns = ['Column Name', 'Nullness']
        data_nullness['Completeness [%]'] = 100 - data_nullness['Nullness']
        plt.figure(figsize=(12, 8), dpi=80)
        sns.barplot(data_nullness.sort_values(by='Completeness [%]', ascending=False), x='Completeness [%]', y='Column Name')
        plt.yticks(rotation=40)
        plt.xlabel(xlabel='Completeness [%]', fontsize=20)
        plt.ylabel(ylabel='')
        plt.title('Augmentation success: completeness [%] by added column')
        plt.show()
