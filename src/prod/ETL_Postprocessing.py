import argparse

import duckdb
from box import Box
from dask import dataframe as dd
from dask.distributed import Client

from util.custom.common import get_dask_cluster
from util.etl.jobs import etl_test_tools

LIST_TABLE_NAME = [
    # 'LEGALLY_OPERATING_BUSINESS',
    # 'PARKING_VIOLATION_ISSUED',
    'WEATHER',
    # 'EVENT'
]


def non_nullness(data: dd.DataFrame,
                 column_name: str):
    return 100 * (len(data) - data[column_name].isnull().sum().compute()) / len(data)


def memory_usage(data: dd.DataFrame,
                 column_name: str):
    return data[column_name].memory_usage(deep=True).compute() / 1000000


if __name__ == '__main__':
    # Read configuration file
    config = Box.from_yaml(filename='config/config.yaml')

    parser = argparse.ArgumentParser(
        prog='ETL Postprocessing Job',
        description='This script takes the .parquet files and makes some transformations to materialize several different insights.')

    arg_env = parser.add_argument("-e",
                                  "--env",
                                  help="Specify environment: 'local'/'hpc'.",
                                  type=str,
                                  default='local')

    args = parser.parse_args()

    if args.env in ('local', 'hpc'):
        environment_name = args.env
    else:
        raise argparse.ArgumentError(argument=arg_env,
                                     message='Invalid argument value for environment. Add the "-h" option to see argument specifications.')

    # Connect to Dask cluster
    cluster = get_dask_cluster(config=config,
                               environment_name=environment_name)

    # Connect to DuckDB
    connection = duckdb.connect()

    with Client(cluster,
                timeout='120s') as client:

        print(f'Dask Scheduler: {cluster.scheduler.address}')
        print(f'Dask Cluster: {cluster.dashboard_link}')

        etl_test_tools(list_table_name=LIST_TABLE_NAME,
                       data_path=config['environment'][environment_name]["data_output_dir"],
                       connection=connection)

        # processed_data_table = []
        # processed_data_column = []
        #
        # for table_name in LIST_TABLE_NAME:
        #
        #     data = read_parquet_table(table_name=table_name)
        #     processed_data_table.append({
        #         'Table Name': table_name,
        #         'Table Size': len(data),
        #         'Freshness [Last Date]': data[DATA_METADATA[table_name]['date_column']].max().compute(),
        #         'Uniqueness [%]': 100 * len(
        #             data[DATA_METADATA[table_name]['identifier_columns']].drop_duplicates().compute()) / len(data)
        #     })
        #     for column_name in data.columns:
        #         processed_data_column.append({
        #             'Table Name': table_name,
        #             'Column Name': column_name,
        #             'Non-Nullness [%]': non_nullness(data=data, column_name=column_name),
        #             'Memory Usage [MB]': memory_usage(data=data, column_name=column_name)
        #         })
        # pd.DataFrame(processed_data_table).to_parquet(f'{config["data_output_dir"]}/PROCESSED_TABLE_HEALTH.parquet')
        # pd.DataFrame(processed_data_column).to_parquet(f'{config["data_output_dir"]}/PROCESSED_COLUMN_HEALTH.parquet')
