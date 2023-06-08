from typing import Union

import duckdb
import pandas as pd
from dask import dataframe as dd


def transform_W(data_W: Union[pd.DataFrame, dd.DataFrame],
                connection: duckdb.DuckDBPyConnection,
                processing_mode: str):
    if processing_mode == 'DuckDB':
        # connection.execute('''
        #         SELECT COUNT(*) - COUNT()
        #         FROM WEATHER
        #         ''')
        pass
    elif processing_mode == 'Dask-SQL':
        pass
    elif processing_mode == 'Dask-Regular':
        pass
    else:
        raise RuntimeError('Unknown processing mode to transform table.')
    # return transformed_data_W

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
              processing_mode: str):
    if table_name == 'WEATHER':
        transformed_data = transform_W(data_W=data,
                                       connection=connection,
                                       processing_mode=processing_mode)

    return transformed_data
