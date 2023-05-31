from dask import dataframe as dd


def load(data: dd.DataFrame,
         table_name: str,
         content_root_path: str = '..') -> None:
    # Store DataFrame in Parquet format
    data.to_parquet(f'{content_root_path}/custom/parquet/{table_name}.parquet')

    # Store DataFrame in HDF5 format
    data.to_hdf(f'{content_root_path}/custom/hdf5/{table_name}-*.hdf',
                            '/custom',
                            mode='w',
                            complib='blosc',
                            complevel=9,
                            min_itemsize=75)
