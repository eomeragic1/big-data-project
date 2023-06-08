from typing import Union

import duckdb
import pandas as pd
from dask import dataframe as dd


def transform_W(data_W: Union[pd.DataFrame, dd.DataFrame],
                connection: duckdb.DuckDBPyConnection,
                processing_mode: str):
    transformed_data_W = data_W
    if processing_mode == 'DuckDB':
        pass
    elif processing_mode == 'Dask-SQL':
        pass
    elif processing_mode == 'Dask-Regular':
        pass
    else:
        raise RuntimeError('Unknown processing mode to transform table.')
    return transformed_data_W


def transform(table_name: str,
              data: Union[pd.DataFrame, dd.DataFrame],
              connection: duckdb.DuckDBPyConnection,
              processing_mode: str):
    if table_name == 'WEATHER':
        transformed_data = transform_W(data_W=data,
                                       connection=connection,
                                       processing_mode=processing_mode)

    return transformed_data
