import dask_jobqueue
from box import Box
from dask import dataframe as dd
from dask.distributed import Client, LocalCluster


def read_parquet_table(table_name: str,
                       data_path: str = 'data/parquet',
                       content_root_path: str = '.'):
    return dd.read_parquet(f'{content_root_path}/{data_path}/{table_name}.parquet')


def dask_config(config: Box) -> Client:
    if config['environment']['name'] == 'local':
        cluster = LocalCluster(
            n_workers=config['cluster']['local']['n_workers'],
            threads_per_worker=config['cluster']['local']['threads_per_worker'])

        client = Client(cluster, timeout="200s")

        return client, cluster

    elif config['environment']['name'] == 'hpc':
        cluster = dask_jobqueue.SLURMCluster(
            processes=config['environment']['hpc']['processes'],
            cores=config['environment']['hpc']['cores'],
            memory=config['environment']['hpc']['memory']
        )

        client = Client(cluster, timeout="120s")

        cluster.scale(jobs=10)
        return client, cluster

    else:
        raise RuntimeError('Unknown environment. Select "hpc" or "local".')
