import dask_jobqueue
from box import Box
from dask import dataframe as dd
from dask.distributed import Client, LocalCluster


def read_parquet_table(table_name: str,
                       data_path: str = 'data/parquet',
                       content_root_path: str = '.'):
    return dd.read_parquet(f'{content_root_path}/{data_path}/{table_name}.parquet')


def get_dask_cluster(config: Box,
                     environment_name: str) -> Client:
    if environment_name == 'local':
        cluster = LocalCluster(
            n_workers=config['cluster'][environment_name]['n_workers'],
            threads_per_worker=config['cluster'][environment_name]['threads_per_worker'])

        return cluster

    elif environment_name == 'hpc':
        cluster = dask_jobqueue.SLURMCluster(
            processes=config['cluster'][environment_name]['processes'],
            cores=config['cluster'][environment_name]['cores'],
            memory=config['cluster'][environment_name]['memory']
        )

        cluster.scale(jobs=10)
        return cluster

    else:
        raise RuntimeError('Unknown environment. Select "hpc" or "local".')
