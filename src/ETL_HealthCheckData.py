import pandas as pd
from box import Box
from dask import dataframe as dd

from util.custom.common import read_parquet_table, get_client
from util.etl.extract import DATA_METADATA

LIST_TABLE_NAME = [
    'LEGALLY_OPERATING_BUSINESS',
    'PARKING_VIOLATION_ISSUED',
    'WEATHER',
    'EVENT'
]


def non_nullness(data: dd.DataFrame,
                 column_name: str):
    return 100 * (len(data) - data[column_name].isnull().sum().compute()) / len(data)


def memory_usage(data: dd.DataFrame,
                 column_name: str):
    return data[column_name].memory_usage(deep=True).compute() / 1000000


if __name__ == '__main__':
    # Read configuration file
    config = Box.from_yaml(filename='../config/config.yaml')
    # Initialize Dask cluster client:
    #   - connects to SLURM if configuration is set to 'hpc'
    client = get_client(config=config)

    processed_data_table = []
    processed_data_column = []

    for table_name in LIST_TABLE_NAME:
        data = read_parquet_table(table_name=table_name)
        processed_data_table.append({
            'Table Name': table_name,
            'Table Size': len(data),
            'Freshness [Last Date]': data[DATA_METADATA[table_name]['date_column']].max().compute(),
            'Uniqueness [%]': 100 * len(
                data[DATA_METADATA[table_name]['identifier_columns']].drop_duplicates().compute()) / len(data)
        })
        for column_name in data.columns:
            processed_data_column.append({
                'Table Name': table_name,
                'Column Name': column_name,
                'Non-Nullness [%]': non_nullness(data=data, column_name=column_name),
                'Memory Usage [MB]': memory_usage(data=data, column_name=column_name)
            })
    pd.DataFrame(processed_data_table).to_parquet('../data/parquet/PROCESSED_TABLE_HEALTH.parquet')
    pd.DataFrame(processed_data_column).to_parquet('../data/parquet/PROCESSED_COLUMN_HEALTH.parquet')

    client.close()
