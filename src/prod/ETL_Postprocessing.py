import argparse
import inspect
import os
import sys

import duckdb
from box import Box
from dask.distributed import Client
from dask_sql import Context

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
parent_parent_dir = os.path.dirname(parent_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, parent_parent_dir)

from util.custom.common import get_dask_cluster
from util.etl.jobs import etl_test_tools

LIST_TABLE_NAME = [
    'LEGALLY_OPERATING_BUSINESS',
    'PARKING_VIOLATION_ISSUED',
    'WEATHER',
    'TRAFFIC',
    'SCHOOLS',
    # 'VIOLATION_COUNTY',
    'REGISTERED_VEHICLES',
    # 'PERMITTED_EVENTS'
]

LIST_FILE_MODE = [
    'parquet',
    'hdf5'
]

LIST_PROCESSING_MODE = [
    'DuckDB',
    'Dask-SQL',
    'Dask-Regular'
]

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
    # Dask SQL context
    context = Context()

    with Client(cluster,
                timeout='120s') as client:

        print(f'Dask Scheduler: {cluster.scheduler.address}')
        print(f'Dask Cluster: {cluster.dashboard_link}')

        if environment_name == 'hpc':
            LIST_PROCESSING_MODE = ['Dask-Regular']
            LIST_FILE_MODE = ['parquet']

        etl_test_tools(list_table_name=LIST_TABLE_NAME,
                       list_processing_mode=LIST_PROCESSING_MODE,
                       list_file_mode=LIST_FILE_MODE,
                       data_path=config['environment'][environment_name]["data_output_dir"],
                       connection=connection,
                       context=context,
                       content_root_path='' if environment_name == 'hpc' else './')
