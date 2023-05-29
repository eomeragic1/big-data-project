import dask.dataframe as dd
import pandas as pd

from util.etl.extract import extract, DATA_METADATA


def transform_LOB(data_LOB: dd.DataFrame) -> dd.DataFrame:
    transformed_data_LOB = data_LOB
    transformed_data_LOB = transformed_data_LOB[transformed_data_LOB['Address State'] == 'NY']
    transformed_data_LOB = transformed_data_LOB[transformed_data_LOB['Address Street Name'].notnull()]

    transformed_data_LOB['License Expiration Date'] = dd.to_datetime(transformed_data_LOB['License Expiration Date'],
                                                                     format='%m/%d/%Y')
    transformed_data_LOB['License Creation Date'] = dd.to_datetime(transformed_data_LOB['License Creation Date'],
                                                                   format='%m/%d/%Y')
    return transformed_data_LOB


def transform_PVI(data_PVI: dd.DataFrame) -> dd.DataFrame:
    transformed_data_PVI = data_PVI
    transformed_data_PVI['Issue Date'] = dd.to_datetime(transformed_data_PVI['Issue Date'],
                                                        format='%m/%d/%Y')

    def transform_vehicle_expiration_date(data: pd.DataFrame):
        data['Vehicle Expiration Date'] = pd.to_datetime(data['Vehicle Expiration Date'],
                                                         errors='coerce',
                                                         format='%Y%m%d')
        return data

    transformed_data_PVI = transformed_data_PVI.map_partitions(transform_vehicle_expiration_date)

    return transformed_data_PVI


def augment(data: dd.DataFrame,
            joining_data: dd.DataFrame,
            joining_table_name: str):
    if joining_table_name == 'LEGALLY_OPERATING_BUSINESS':
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on='Street Name',
            right_on='Address Street Name',
            how='left'
        )
    return transformed_data


def extract_transform(table_name: str):
    data = extract(table_name=table_name)
    if table_name == 'LEGALLY_OPERATING_BUSINESS':
        transformed_data = transform_LOB(data_LOB=data)
    elif table_name == 'PARKING_VIOLATION_ISSUED':
        transformed_data = transform_PVI(data_PVI=data)
    else:
        raise RuntimeError('Unknown dataset.')
    transformed_data = transformed_data[DATA_METADATA[table_name]['included_columns']]
    return transformed_data
