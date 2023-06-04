import dask_jobqueue
from box import Box
from dask import dataframe as dd
from dask.distributed import Client, LocalCluster


def read_parquet_table(table_name: str,
                       data_path: str = 'data/parquet',
                       content_root_path: str = '.'):
    return dd.read_parquet(f'{content_root_path}/{data_path}/{table_name}.parquet')


def dask_config(config: Box,
                environment_name: str) -> Client:
    if environment_name == 'local':
        cluster = LocalCluster(
            n_workers=config['cluster'][environment_name]['n_workers'],
            threads_per_worker=config['cluster'][environment_name]['threads_per_worker'])

        client = Client(cluster, timeout="200s")

        return client, cluster

    elif environment_name == 'hpc':
        cluster = dask_jobqueue.SLURMCluster(
            processes=config['environment'][environment_name]['processes'],
            cores=config['environment'][environment_name]['cores'],
            memory=config['environment'][environment_name]['memory']
        )

        client = Client(cluster, timeout="120s")

        cluster.scale(jobs=10)
        return client, cluster

    else:
        raise RuntimeError('Unknown environment. Select "hpc" or "local".')
