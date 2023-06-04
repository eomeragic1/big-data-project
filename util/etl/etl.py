import time
import tracemalloc

from util.custom.common import read_parquet_table
from util.etl.load import load
from util.etl.transform import extract_transform, augment


def etl_single_table_transformations(list_table_name: list,
                                     input_data_path: str,
                                     output_data_path: str):
    for table_name in list_table_name:
        print(f'Starting single table transformation for "{table_name}"...')
        tracemalloc.start()
        start_time = time.time()

        # Extract and transform custom
        transformed_data = extract_transform(table_name=table_name,
                                             data_path=input_data_path)

        # Load custom to HDF5 and Parquet
        load(data=transformed_data,
             table_name=table_name,
             data_path=output_data_path)

        peak_memory_usage = round(tracemalloc.get_traced_memory()[1] / 1000000, 2)
        execution_time_in_s = round(time.time() - start_time, 2)
        print(f'Table {table_name}: {peak_memory_usage}MB | {execution_time_in_s}s')

        tracemalloc.stop()


def etl_augmentation(list_table_name: list):
    tracemalloc.start()
    start_time = time.time()

    data = read_parquet_table(table_name='PARKING_VIOLATION_ISSUED')

    for table_name in list_table_name:
        joining_data = read_parquet_table(table_name=table_name)
        data = augment(data=data,
                       joining_data=joining_data,
                       joining_table_name=table_name)

    load(data=data,
         table_name='AUGMENTED_PARKING_VIOLATION_ISSUED',
         data_path='data')

    peak_memory_usage = round(tracemalloc.get_traced_memory()[1] / 1000000, 2)
    execution_time_in_s = round(time.time() - start_time, 2)
    print(f'Data Augmentation: {peak_memory_usage}MB | {execution_time_in_s}s')

    tracemalloc.stop()
