import time
import tracemalloc
from typing import Union

import duckdb
import pandas as pd
from dask import dataframe as dd
from dask_sql import Context

from util.etl.initial.extract import DATA_METADATA


def transform_dask_sql(data: dd.DataFrame,
                       table_name: str,
                       context: Context):
    nullness_columns = ','.join([f'100 * (COUNT(1) - COUNT("{col}")) / COUNT(1) AS "{col}"' for col in data.columns])
    data_nullness = context.sql(f'''
                        SELECT {nullness_columns}
                        FROM {table_name} 
                        ''').compute().T.reset_index()
    data_nullness.columns = ['Column Name', 'Nullness']
    data_nullness['Table Name'] = len(data_nullness) * ['WEATHER']

    # Calculating ROW COUNT
    data_row_count = context.sql(f'''
                        SELECT COUNT(1) AS ROW_COUNT
                        FROM {table_name} 
                        ''').compute().iloc[0]['row_count']

    data_row_count_by_date = None
    # Calculating ROW COUNT BY DATE
    if DATA_METADATA[table_name]['date_column'] is not None:
        data_row_count_by_date = context.sql(f'''
                            SELECT "{DATA_METADATA[table_name]['date_column']}" AS DATE,
                                COUNT(1) AS ROW_COUNT
                            FROM {table_name} 
                            GROUP BY "{DATA_METADATA[table_name]['date_column']}"
                            ''').compute()

    return {
        'nullness': data_nullness,
        'row_count': data_row_count,
        'row_count_by_date': data_row_count_by_date
    }


def transform_duckdb(data: pd.DataFrame,
                     table_name: str,
                     connection: duckdb.DuckDBPyConnection):
    # Calculating NULNESS
    nullness_columns = ','.join([f'100 * (COUNT(1) - COUNT("{col}")) / COUNT(1) AS "{col}"' for col in data.columns])
    data_nullness = connection.execute(f'''
                    SELECT {nullness_columns}
                    FROM {table_name} 
                    ''').fetchdf().T.reset_index()
    data_nullness.columns = ['Column Name', 'Nullness']
    data_nullness['Table Name'] = len(data_nullness) * ['WEATHER']

    # Calculating ROW COUNT
    data_row_count = connection.execute(f'''
                    SELECT COUNT(1) AS ROW_COUNT
                    FROM {table_name} 
                    ''').fetchone()[0]

    data_row_count_by_date = None
    # Calculating ROW COUNT BY DATE
    if DATA_METADATA[table_name]['date_column'] is not None:
        data_row_count_by_date = connection.execute(f'''
                        SELECT "{DATA_METADATA[table_name]['date_column']}" AS DATE,
                            COUNT(1) AS ROW_COUNT
                        FROM {table_name} 
                        GROUP BY "{DATA_METADATA[table_name]['date_column']}"
                        ''').fetchall()

    return {
        'nullness': data_nullness,
        'row_count': data_row_count,
        'row_count_by_date': data_row_count_by_date
    }


def transform_dask_regular(data: dd.DataFrame,
                           table_name: str):
    data_nullness = pd.DataFrame(
        [{col: 100 * (data[col].isnull().sum().compute()) / data.shape[0].compute() for col in data.columns}]
    ).T.reset_index()
    data_nullness.columns = ['Column Name', 'Nullness']
    data_nullness['Table Name'] = len(data_nullness) * [table_name]

    data_row_count = data.shape[0].compute()

    data_row_count_by_date = None
    if DATA_METADATA[table_name]['date_column'] is not None:
        data_row_count_by_date = data \
            .groupby(by=DATA_METADATA[table_name]['date_column'])[DATA_METADATA[table_name]['date_column']] \
            .count() \
            .compute()

    return {
        'nullness': data_nullness,
        'row_count': data_row_count,
        'row_count_by_date': data_row_count_by_date
    }


# Transform for postprocessing
# Here, we want to be able to deduce health checks on processed tables and columns. We want to derive the following metrics:
# - Row count by date
# - Unique row count
# - Non-nullness  by column
# - Processing time
# - Processing memory usage
def transform(table_name: str,
              data: Union[pd.DataFrame, dd.DataFrame],
              connection: duckdb.DuckDBPyConnection,
              context: Context,
              processing_mode: str):
    tracemalloc.start()

    start_time = time.time()
    if processing_mode == 'DuckDB':
        results = transform_duckdb(data=data,
                                   table_name=table_name,
                                   connection=connection)
    elif processing_mode == 'Dask-SQL':
        results = transform_dask_sql(data=data,
                                     table_name=table_name,
                                     context=context)
    elif processing_mode == 'Dask-Regular':
        results = transform_dask_regular(data=data,
                                         table_name=table_name)
    else:
        raise RuntimeError('Unknown processing mode to transform table.')

    peak_memory_usage = round(tracemalloc.get_traced_memory()[1] / 1000000, 2)
    execution_time_in_s = round(time.time() - start_time, 2)

    tracemalloc.start()

    results['Peak memory usage'] = peak_memory_usage
    results['Execution time'] = execution_time_in_s
    return results
