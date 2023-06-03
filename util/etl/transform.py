import datetime

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

    def transform_violation_time(data: pd.DataFrame):
        data['Violation Time'] = list(
            map(
                lambda x: 0 if x is None else
                (int(x[:2]) + (0 if x[4] == 'A' else 12))
            )
        )
        return data

    transformed_data_PVI['Violation Time'] = transformed_data_PVI.map_partitions(transform_violation_time)

    def transform_vehicle_expiration_date(data: pd.DataFrame):
        data['Vehicle Expiration Date'] = pd.to_datetime(data['Vehicle Expiration Date'],
                                                         errors='coerce',
                                                         format='%Y%m%d')
        return data

    transformed_data_PVI = transformed_data_PVI.map_partitions(transform_vehicle_expiration_date)

    return transformed_data_PVI


def transform_E(data_E: dd.DataFrame) -> dd.DataFrame:
    transformed_data_E = data_E
    transformed_data_E['date'] = dd.to_datetime(transformed_data_E['date'],
                                                format='%m/%d/%Y')
    return data_E


def transform_W(data_W: dd.DataFrame) -> dd.DataFrame:
    transformed_data_W = data_W
    transformed_data_W['Time'] = dd.to_datetime(transformed_data_W['Time'])

    return data_W


def transform_RV(data_RV: dd.DataFrame) -> dd.DataFrame:
    transformed_data_RV = data_RV
    transformed_data_RV = transformed_data_RV.loc[
                          (transformed_data_RV['Record Type'] == 'VEH') | (transformed_data_RV['Record Type'] == 'TRL'),
                          :]
    transformed_data_RV['Reg Expiration Date'] = dd.to_datetime(transformed_data_RV['date'],
                                                                format='%m/%d/%Y')
    transformed_data_RV['Reg Valid Date'] = dd.to_datetime(transformed_data_RV['date'],
                                                           format='%m/%d/%Y')
    transformed_data_RV = transformed_data_RV.loc[
                          transformed_data_RV['Reg Expiration Date'] > datetime.datetime(2022, 6, 1), :]
    return transformed_data_RV


def augment(data: dd.DataFrame,
            joining_data: dd.DataFrame,
            joining_table_name: str):
    if joining_table_name == 'LEGALLY_OPERATING_BUSINESS':
        joining_data = joining_data.add_prefix('A - ')
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on='Street Name',
            right_on='A - Address Street Name',
            how='left'
        )
    elif joining_table_name == 'WEATHER':
        joining_data = joining_data.add_prefix('A - ')
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on='Street Name',
            right_on='A - Address Street Name',
            how='left'
        )
    return transformed_data


def extract_transform(table_name: str, data_path: str):
    data = extract(table_name=table_name, data_path=data_path)

    # data_VC = dd.read_csv(DATA_METADATA['VIOLATION_COUNTY'][''])

    if table_name == 'LEGALLY_OPERATING_BUSINESS':
        # TODO: transform streets
        transformed_data = transform_LOB(data_LOB=data)
    elif table_name == 'PARKING_VIOLATION_ISSUED':
        transformed_data = transform_PVI(data_PVI=data)
    elif table_name == 'WEATHER':
        transformed_data = transform_W(data_W=data)
    elif table_name == 'EVENT':
        transformed_data = transform_E(data_E=data)
    elif table_name == 'REGISTERED_VEHICLES':
        transformed_data = transform_RV(data_RV=data)
    else:
        raise RuntimeError('Unknown dataset.')

    if DATA_METADATA[table_name]['date_column'] is not None:
        transformed_data.loc[
        (transformed_data[DATA_METADATA[table_name]['date_column']] > datetime.datetime(2022, 6, 1)) & (
                transformed_data[DATA_METADATA[table_name]['date_column']] < datetime.datetime(2023, 5, 31)), :]

    transformed_data = transformed_data[DATA_METADATA[table_name]['included_columns']]
    return transformed_data
