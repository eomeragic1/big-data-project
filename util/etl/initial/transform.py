import datetime

import dask.dataframe as dd
import pandas as pd
import numpy as np

from util.etl.initial.table.parking_violation_issued import rename_PVI
from util.etl.initial.extract import extract, DATA_METADATA


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
    transformed_data_PVI = rename_PVI(data_PVI)
    transformed_data_PVI['Issue Date'] = dd.to_datetime(transformed_data_PVI['Issue Date'],
                                                        format='%m/%d/%Y', errors='coerce')
    transformed_data_PVI = transformed_data_PVI.loc[
                           (transformed_data_PVI['Issue Date'] >= datetime.datetime(2013, 6, 1)) & (
                                       transformed_data_PVI['Issue Date'] <= datetime.datetime.today()), :]

    def transform_violation_time_instance(x: str):
        if not x or len(x) != 5 or ' ' in x:
            return ''
        try:
            if x[4] not in ['A', 'P'] or int(x[2:4]) > 59 or int(x[:2]) > 12:
                return ''
        except ValueError:
            return ''
        return x

    def transform_violation_time_partition(data: pd.DataFrame):
        return data.apply(transform_violation_time_instance)

    transformed_data_PVI['Violation Time'] = transformed_data_PVI['Violation Time'].astype(str)
    transformed_data_PVI['Violation Time'] = transformed_data_PVI['Violation Time'].map_partitions(
        transform_violation_time_partition, meta=('Violation Time', str)).replace('', np.nan)
    transformed_data_PVI = transformed_data_PVI.dropna(subset=['Violation Time'])
    transformed_data_PVI['Violation Time'] = dd.to_datetime(
        transformed_data_PVI['Violation Time'].apply(lambda x: f'{str(x) if x[:2] != "00" else "12" + x[2:]}M',
                                                     meta=('Violation Time', str)), format='%I%M%p',
        errors='coerce').apply(
        lambda x: pd.to_datetime(x, unit='s').hour, meta=('Violation Time', int))

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
    return transformed_data_E


def transform_W(data_W: dd.DataFrame) -> dd.DataFrame:
    transformed_data_W = data_W
    transformed_data_W['Time'] = dd.to_datetime(transformed_data_W['Time'])
    transformed_data_W['Date'] = transformed_data_W['Time'].dt.date
    transformed_data_W['Time'] = transformed_data_W['Time'].dt.hour
    transformed_data_W['Date'] = dd.to_datetime(transformed_data_W['Date'], format='%Y-%m-%d')
    transformed_data_W['Violation County Name'] = transformed_data_W['Borough'].replace('StatenIsland', 'Staten Island')
    return transformed_data_W


def transform_RV(data_RV: dd.DataFrame) -> dd.DataFrame:
    transformed_data_RV = data_RV
    transformed_data_RV = transformed_data_RV.loc[
                          (transformed_data_RV['Record Type'] == 'VEH') | (transformed_data_RV['Record Type'] == 'TRL'),
                          :]
    transformed_data_RV['Reg Expiration Date'] = dd.to_datetime(transformed_data_RV['Reg Expiration Date'],
                                                                format='%m/%d/%Y')
    transformed_data_RV['Reg Valid Date'] = dd.to_datetime(transformed_data_RV['Reg Valid Date'],
                                                           format='%m/%d/%Y')
    transformed_data_RV = transformed_data_RV.loc[
                          (transformed_data_RV['Reg Expiration Date'] > datetime.datetime(2022, 6, 1)) & (
                                  transformed_data_RV['Reg Expiration Date'] > datetime.datetime.now()), :]

    grouped_df = transformed_data_RV.groupby(['Make']).size().reset_index()
    grouped_df.columns = ['Vehicle Make', 'Count']

    # Compute the resulting DataFrame
    return grouped_df


def transform_PE(data_PE: dd.DataFrame) -> dd.DataFrame:
    transformed_data_PE = data_PE
    transformed_data_PE['Start Date/Time'] = dd.to_datetime(transformed_data_PE['Start Date/Time'],
                                                            format='%m/%d/%Y %I:%M:%S %p')
    transformed_data_PE['End Date/Time'] = dd.to_datetime(transformed_data_PE['End Date/Time'],
                                                          format='%m/%d/%Y %I:%M:%S %p')
    filtered_data = transformed_data_PE.loc[
                    (transformed_data_PE['End Date/Time'] >= datetime.datetime(2013, 6, 1)) & (
                            transformed_data_PE['Start Date/Time'] <= datetime.datetime.now()),
                    :].reset_index(drop=True)
    filtered_data['Date'] = filtered_data.apply(
        lambda row: pd.date_range(start=row['Start Date/Time'], end=row['End Date/Time'], freq='D'), axis=1)

    filtered_data = filtered_data.explode('Date')
    filtered_data['Date'] = dd.to_datetime(filtered_data['Date'], format='%Y-%m-%d %H:%M:%S')
    filtered_data['Date'] = filtered_data['Date'].dt.date
    # Step 4: Group by date and borough, and compute the count of active events
    result = filtered_data.groupby(['Date', 'Event Borough']).size().reset_index()
    result.columns = ['Date', 'Event Borough', 'Events Count']
    result['Date'] = dd.to_datetime(result['Date'])
    result = result.loc[
             (result['Date'] >= datetime.datetime(2013, 6, 1)) & (result['Date'] <= datetime.datetime.today()), :]
    result = result.sort_values(by=['Date', 'Event Borough'])

    return result


def transform_S(data_S: dd.DataFrame) -> dd.DataFrame:
    transformed_data_S = data_S
    transformed_data_S = transformed_data_S.dropna(subset=['Police_precinct'])  # 9 examples where precinct is NA
    transformed_data_S['Police_precinct'] = transformed_data_S['Police_precinct'].astype(int)
    grouped = transformed_data_S.groupby('Police_precinct').size().reset_index()
    grouped.columns = ['Police Precinct', 'Number of Schools']
    return grouped


def transform_T(data_T: dd.DataFrame) -> dd.DataFrame:
    transformed_data_T = data_T
    transformed_data_T['Date'] = dd.to_datetime(transformed_data_T['Date'], format='%m/%d/%Y')
    transformed_data_T = transformed_data_T.loc[transformed_data_T['Date'] >= datetime.datetime(2013, 6, 1), :]
    transformed_data_T = transformed_data_T.groupby(['Date', 'Hour', 'Direction']).agg(
        {'# Vehicles - E-ZPass': 'sum', '# Vehicles - VToll': 'sum'}).sum(axis=1).reset_index()
    transformed_data_T.columns = ['Date', 'Hour', 'Direction', 'Average']
    transformed_data_T = transformed_data_T.groupby(['Hour', 'Direction']).agg({'Average': 'mean'}).reset_index()
    transformed_data_T.columns = ['Hour', 'Direction', 'Average']
    return transformed_data_T


def augment(data: dd.DataFrame,
            joining_data: dd.DataFrame,
            joining_table_name: str):
    if joining_table_name == 'VIOLATION_COUNTY':
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on='Violation County',
            right_on='Violation County Code',
            how='inner'
        )
    elif joining_table_name == 'PERMITTED_EVENTS':
        joining_data = joining_data.add_prefix('PE - ')
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on=['Issue Date', 'Violation County Name'],
            right_on=['PE - Date', 'PE - Event Borough'],
            how='left'
        )
    elif joining_table_name == 'WEATHER':
        joining_data = joining_data.add_prefix('W - ')
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on=['Issue Date', 'Violation Time', 'Violation County Name'],
            right_on=['W - Date', 'W - Time', 'W - Borough'],
            how='inner'
        )
    elif joining_table_name == 'LEGALLY_OPERATING_BUSINESS':
        joining_data = joining_data.add_prefix('A - ')
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on='Street Name',
            right_on='A - Address Street Name',
            how='left'
        )
    elif joining_table_name == 'SCHOOLS':
        joining_data = joining_data.add_prefix('S - ')
        transformed_data = dd.merge(
            left=data,
            right=joining_data,
            left_on='Violation Precinct',
            right_on='S - Police Precinct',
            how='left'
        )
    else:
        return data

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
    elif table_name == 'PERMITTED_EVENTS':
        transformed_data = transform_PE(data_PE=data)
    elif table_name == 'VIOLATION_COUNTY':
        transformed_data = data
    elif table_name == 'SCHOOLS':
        transformed_data = transform_S(data_S=data)
    elif table_name == 'TRAFFIC':
        transformed_data = transform_T(data_T=data)
    elif table_name == 'PARKING_VIOLATION_CODES':
        transformed_data = data
    else:
        raise RuntimeError('Unknown dataset.')

    transformed_data = transformed_data[DATA_METADATA[table_name]['included_columns']]
    return transformed_data
