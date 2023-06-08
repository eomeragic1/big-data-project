from dask import dataframe as dd


def load(data: dd.DataFrame,
         table_name: str,
         data_path: str) -> None:
    # Store DataFrame in Parquet format
    data.to_parquet(path=f'{data_path}/parquet/{table_name}.parquet',
                    overwrite=True,
                    engine='pyarrow',
                    compression='gzip')

    # Store DataFrame in HDF5 format
    data.to_hdf(path_or_buf=f'{data_path}/hdf5/{table_name}-*.hdf',
                key='/data',
                mode='w',
                complib='blosc',
                complevel=9,
                min_itemsize=75)
