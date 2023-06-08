from util.custom.common import read_hdf5_table


def extract(table_name: str,
            file_mode: str,
            processing_mode: str,
            data_path: str):
    input_data_path = f'{data_path}/{file_mode}'
    engine = 'pandas' if processing_mode == 'DuckDB' else 'dask'
    if file_mode == 'hdf5':
        data = read_hdf5_table(table_name=table_name,
                               data_path=input_data_path,
                               content_root_path='.',
                               engine=engine)
    elif file_mode == 'parquet':
        data = read_hdf5_table(table_name=table_name,
                               data_path=input_data_path,
                               content_root_path='.',
                               engine=engine)
    else:
        raise RuntimeError('Unknown file mode to extract table.')

    return data
