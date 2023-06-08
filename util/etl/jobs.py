import itertools
import time
import tracemalloc

import duckdb
import pandas as pd
from box import Box
from dask_sql import Context
from distributed import performance_report

import util.etl.initial.load as load_initial
import util.etl.initial.transform as transform_initial
import util.etl.postprocessing.extract as extract_postprocessing
import util.etl.postprocessing.transform as transform_postprocessing
from util.custom.common import read_parquet_table

LIST_FILE_MODE = [
    'parquet',
    # 'hdf5'
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
            transformed_data = transform_initial.extract_transform(table_name=table_name,
                                                                   data_path=input_data)

            # Load custom to HDF5 and Parquet
            load_initial.load(data=transformed_data,
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
        data = transform_initial.augment(data=data,
                                         joining_data=joining_data,
                                         joining_table_name=table_name)

    load_initial.load(data=data,
                      table_name='AUGMENTED_PARKING_VIOLATION_ISSUED',
                      data_path=data_path)

    peak_memory_usage = round(tracemalloc.get_traced_memory()[1] / 1000000, 2)
    execution_time_in_s = round(time.time() - start_time, 2)
    print(f'Data Augmentation: {peak_memory_usage}MB | {execution_time_in_s}s')

    tracemalloc.stop()


def etl_test_tools(list_table_name: list,
                   data_path: str,
                   connection: duckdb.DuckDBPyConnection,
                   context: Context,
                   content_root_path: str = '.'):
    results = dict(
        data_nullness=list(),
        data_row_count_by_date=list(),
        data_metadata=list()

    )
    for table_name, file_mode, processing_mode in itertools.product(list_table_name,
                                                                    LIST_FILE_MODE,
                                                                    LIST_PROCESSING_MODE):
        data = extract_postprocessing.extract(table_name=table_name,
                                              file_mode=file_mode,
                                              processing_mode=processing_mode,
                                              content_root_path=content_root_path,
                                              data_path=data_path)
        if processing_mode == 'DuckDB':
            connection.register(table_name, data)

        if processing_mode == 'Dask-SQL':
            context.create_table(table_name, data)

        transformed_data = transform_postprocessing.transform(data=data,
                                                              table_name=table_name,
                                                              connection=connection,
                                                              context=context,
                                                              processing_mode=processing_mode)
        results['data_metadata'].append({
            'Table': table_name,
            'File mode': file_mode,
            'Processing mode': processing_mode,
            'Execution time': transformed_data['Execution time'],
            'Peak memory usage': transformed_data['Peak memory usage'],
            'Row count ': transformed_data['row_count']
        })
        if file_mode == 'parquet' and processing_mode == 'Dask-Regular':
            results['data_nullness'].extend([transformed_data['nullness']])
            results['data_row_count_by_date'].extend([transformed_data['row_count_by_date']])

    data_nullness = pd.concat(results['data_nullness'], axis=0)
    data_row_count_by_date = pd.concat(results['data_row_count_by_date'], axis=0)
    data_metadata = pd.DataFrame(results['data_metadata'])

    load_initial.load(data=data_nullness, table_name='PROCESSED_COLUMN_NULLNESS', data_path=data_path)
    load_initial.load(data=data_row_count_by_date, table_name='PROCESSED_COUNT_BY_DATE', data_path=data_path)
    load_initial.load(data=data_metadata, table_name='PROCESSED_TABLE_METADATA', data_path=data_path)
