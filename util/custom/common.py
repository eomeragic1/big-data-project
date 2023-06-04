import dask_jobqueue
from box import Box
from dask import dataframe as dd
import dask
from dask.distributed import Client, LocalCluster


def read_parquet_table(table_name: str,
                       data_path: str = 'data/parquet',
                       content_root_path: str = '.'):
    return dd.read_parquet(f'{content_root_path}/{data_path}/{table_name}.parquet')


def dask_config(config: Box) -> Client:
    if config['environment']['name'] == 'local':
        cluster = LocalCluster(n_workers=4,
                               threads_per_worker=2)

        client = Client(cluster, timeout="200s")
        
        cluster.scale(jobs=10)
        return client, cluster

    elif config['environment']['name'] == 'hpc':
        cluster = dask_jobqueue.SLURMCluster(
            processes=2,
            cores=8,
            memory='16 GB'
        )

        client = Client(cluster, timeout="120s")

        cluster.scale(jobs=10)
        return client, cluster

    else:
        raise RuntimeError('Unknown environment. Select "hpc" or "local".')
