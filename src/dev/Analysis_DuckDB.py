import duckdb
import pandas as pd
from box import Box
from dask.distributed import Client
from dask import dataframe as dd
from util.custom.common import get_dask_cluster

if __name__ == '__main__':
    # Read configuration file
    config = Box.from_yaml(filename='config/config.yaml')

    # Initialize Dask cluster client:
    #   - connects to SLURM if configuration is set to 'hpc'

    print('Initializing Dask client...')

    connection = duckdb.connect()

    data = pd.read_parquet('data/parquet/WEATHER.parquet')
    connection.register('test',data)
    tmp = connection.execute('select count(1) from test').fetchdf()
    print(tmp)