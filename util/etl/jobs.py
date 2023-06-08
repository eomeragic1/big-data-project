import itertools
import time
import tracemalloc

import duckdb
from box import Box
from distributed import performance_report

from util.custom.common import read_parquet_table
from util.etl import initial as etl_initial
from util.etl import postprocessing as etl_postprocessing

LIST_FILE_MODE = [
    'parquet',
    'hdf5'
]

LIST_PROCESSING_MODE = [
    'DuckDB',
    'Dask-SQL',
    'Dask-Regular',
]


def etl_single_table_transformations(list_table_name: list,
                                     config: Box,
                                     environment_name: str):
    input_data_path_original = config['environment'][environment_name]['data_original_dir']
    input_data_path_augmentation = config['environment'][environment_name]['data_augmentation_dir']
    output_data_path = config['environment'][environment_name]['data_output_dir']
    for table_name in list_table_name:
        with performance_report(filename=f"assets/Performance Reports/{table_name}-Performance-Report.html"):
            print(f'Starting single table transformation for "{table_name}"...')
            tracemalloc.start()
            start_time = time.time()

            # Extract and transform custom
            input_data = input_data_path_augmentation if table_name != 'PARKING_VIOLATION_ISSUED' else input_data_path_original
            transformed_data = etl_initial.transform.extract_transform(table_name=table_name,
                                                                       data_path=input_data)

            # Load custom to HDF5 and Parquet
            etl_initial.load.load(data=transformed_data,
                                  table_name=table_name,
                                  data_path=output_data_path)

            peak_memory_usage = round(tracemalloc.get_traced_memory()[1] / 1000000, 2)
            execution_time_in_s = round(time.time() - start_time, 2)
            print(f'Table {table_name}: {peak_memory_usage}MB | {execution_time_in_s}s')

            tracemalloc.stop()


def etl_augmentation(list_table_name: list, data_path: str, content_root_path: str):
    tracemalloc.start()
    start_time = time.time()

    data = read_parquet_table(table_name='PARKING_VIOLATION_ISSUED', data_path=data_path + '/parquet',
                              content_root_path=content_root_path)
    # For HDF5:
    # data = read_hdf5_table(table_name='PARKING_VIOLATIN_ISSUED')
    for table_name in list_table_name:
        # FOR HDF5:
        # joining_data = read_hdf5_table(table_name=table_name)
        joining_data = read_parquet_table(table_name=table_name, data_path=data_path + '/parquet',
                                          content_root_path=content_root_path)
        data = etl_initial.transform.augment(data=data,
                                             joining_data=joining_data,
                                             joining_table_name=table_name)

    etl_initial.load.load(data=data,
                          table_name='AUGMENTED_PARKING_VIOLATION_ISSUED',
                          data_path='data')

    peak_memory_usage = round(tracemalloc.get_traced_memory()[1] / 1000000, 2)
    execution_time_in_s = round(time.time() - start_time, 2)
    print(f'Data Augmentation: {peak_memory_usage}MB | {execution_time_in_s}s')

    tracemalloc.stop()


def etl_test_tools(list_table_name: list,
                   data_path: str,
                   content_root_path: str,
                   connection: duckdb.DuckDBPyConnection):
    for table_name, file_mode, processing_mode in itertools.combinations(list_table_name,
                                                                         LIST_FILE_MODE,
                                                                         LIST_PROCESSING_MODE):
        data = etl_postprocessing.extract.extract(table_name=table_name,
                                                  file_mode=file_mode,
                                                  processing_mode=processing_mode,
                                                  data_path=f'{content_root_path}/{data_path}')
        if processing_mode == 'DuckDB':
            connection.register(table_name, data)

        transformed_data = etl_postprocessing.transform.transform(data=data,
                                                                  table_name=table_name,
                                                                  connection=connection,
                                                                  processing_mode=processing_mode)
