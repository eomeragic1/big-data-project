import dask_jobqueue
from box import Box
from dask import dataframe as dd
from dask.distributed import Client


def read_parquet_table(table_name: str,
                       data_path: str = 'data/parquet',
                       content_root_path: str = '..'):
    return dd.read_parquet(f'{content_root_path}/{data_path}/{table_name}.parquet')


def get_client(config: Box) -> Client:
    if config['environment']['name'] == 'local':
        return Client(n_workers=4, threads_per_worker=2, timeout="200s")
    elif config['environment']['name'] == 'hpc':
        cluster = dask_jobqueue.SLURMCluster(
            queue='all',
            processes=2,
            cores=16,
            memory='32GB',
            scheduler_options={'dashboard_address': ':8087'},
            death_timeout=120
        )
        return Client(cluster, timeout="120s")
    else:
        raise RuntimeError('Unknown environment. Select "hpc" or "local".')
