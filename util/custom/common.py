import dask_jobqueue
import pandas as pd
from box import Box
from dask import dataframe as dd
from dask.distributed import Client, LocalCluster


def read_parquet_table(table_name: str,
                       data_path: str = 'data/augmentation/parquet',
                       content_root_path: str = '.',
                       engine: str = 'dask'):
    if engine == 'dask':
        return dd.read_parquet(f'{content_root_path}{data_path}/{table_name}.parquet')
    elif engine == 'pandas':
        return pd.read_parquet(f'{content_root_path}{data_path}/{table_name}.parquet')
    else:
        raise RuntimeError('Unknown engine to read parquet table.')


def read_hdf5_table(table_name: str,
                    data_path: str = 'data/hdf5',
                    content_root_path: str = '.',
                    engine: str = 'dask'):
    data = dd.read_hdf(pattern=f'{content_root_path}{data_path}/{table_name}-*.hdf', key='/data')
    if engine == 'pandas':
        data = data.compute()
    elif engine != 'dask':
        raise RuntimeError('Unknown engine to read HDF5 table.')
    return data


def get_dask_cluster(config: Box,
                     environment_name: str) -> Client:
    if environment_name == 'local':
        cluster = LocalCluster(
            n_workers=config['cluster'][environment_name]['n_workers'],
            threads_per_worker=config['cluster'][environment_name]['threads_per_worker'])

        return cluster

    elif environment_name == 'hpc':
        cluster = dask_jobqueue.SLURMCluster(
            queue='all',
            processes=config['cluster'][environment_name]['processes'],
            cores=config['cluster'][environment_name]['cores'],
            memory=config['cluster'][environment_name]['memory']
        )

        cluster.scale(jobs=config['cluster'][environment_name]['jobs'])
        return cluster

    else:
        raise RuntimeError('Unknown environment. Select "hpc" or "local".')
