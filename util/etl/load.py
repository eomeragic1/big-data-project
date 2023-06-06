from dask import dataframe as dd


def load(data: dd.DataFrame,
         table_name: str,
         data_path: str) -> None:
    # Store DataFrame in Parquet format
    data.head(1000).to_parquet(f'{data_path}/parquet/{table_name}.parquet')

    # Store DataFrame in HDF5 format
    data.to_hdf(f'{data_path}/hdf5/{table_name}-*.hdf',
                '/data',
                mode='w',
                complib='blosc',
                complevel=9,
                min_itemsize=75)
