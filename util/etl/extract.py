import glob

import dask.dataframe as dd

from util.custom.event import TABLE_NAME_EVENT, DTYPES_EVENT, INCLUDED_COLUMNS_EVENT
from util.custom.legally_operating_business import \
    DTYPES_LEGALLY_OPERATING_BUSINESS, \
    TABLE_NAME_LEGALLY_OPERATING_BUSINESS, \
    INCLUDED_COLUMNS_LEGALLY_OPERATING_BUSINESS
from util.custom.parking_violation_issued import \
    DTYPES_PARKING_VIOLATION_ISSUED, \
    TABLE_NAME_PARKING_VIOLATION_ISSUED, \
    INCLUDED_COLUMNS_PARKING_VIOLATION_ISSUED
from util.custom.permitted_events import TABLE_NAME_PERMITTED_EVENTS, DTYPES_PERMITTED_EVENTS, \
    INCLUDED_COLUMNS_PERMITTED_EVENTS
from util.custom.registered_vehicles import TABLE_NAME_REGISTERED_VEHICLES, DTYPES_REGISTERED_VEHICLES, \
    INCLUDED_COLUMNS_REGISTERED_VEHICLES
from util.custom.schools import TABLE_NAME_SCHOOLS, DTYPES_SCHOOLS, INCLUDED_COLUMNS_SCHOOLS
from util.custom.traffic import INCLUDED_COLUMNS_TRAFFIC, TABLE_NAME_TRAFFIC, DTYPES_TRAFFIC
from util.custom.weather import TABLE_NAME_WEATHER, DTYPES_WEATHER, INCLUDED_COLUMNS_WEATHER

DTYPES_VIOLATION_CODE = {
    'Violation County Code': 'str',
    'Violation County Name': 'str',
    'Violation County File Name Suffix': 'str'
}

DATA_METADATA = {
    TABLE_NAME_LEGALLY_OPERATING_BUSINESS: {
        'filename': f'{TABLE_NAME_LEGALLY_OPERATING_BUSINESS}',
        'dtypes': DTYPES_LEGALLY_OPERATING_BUSINESS,
        'columns': list(DTYPES_LEGALLY_OPERATING_BUSINESS.keys()),
        'default_column': list(DTYPES_LEGALLY_OPERATING_BUSINESS.keys())[0],
        'included_columns': INCLUDED_COLUMNS_LEGALLY_OPERATING_BUSINESS,
        'identifier_columns': ['DCA License Number', 'License Creation Date'],
        'date_column': 'License Creation Date'
    },
    TABLE_NAME_PARKING_VIOLATION_ISSUED: {
        'filename': f'{TABLE_NAME_PARKING_VIOLATION_ISSUED}',
        'dtypes': DTYPES_PARKING_VIOLATION_ISSUED,
        'columns': list(DTYPES_PARKING_VIOLATION_ISSUED.keys()),
        'default_column': list(DTYPES_PARKING_VIOLATION_ISSUED.keys())[0],
        'included_columns': INCLUDED_COLUMNS_PARKING_VIOLATION_ISSUED,
        'identifier_columns': ['Summons Number', 'Issue Date'],
        'date_column': 'Issue Date'
    },
    TABLE_NAME_WEATHER: {
        'filename': f'{TABLE_NAME_WEATHER}',
        'dtypes': DTYPES_WEATHER,
        'columns': list(DTYPES_WEATHER.keys()),
        'default_column': list(DTYPES_WEATHER.keys())[0],
        'included_columns': INCLUDED_COLUMNS_WEATHER,
        'identifier_columns': ['Time', 'Borough'],
        'date_column': None
    },
    TABLE_NAME_EVENT: {
        'filename': f'{TABLE_NAME_EVENT}',
        'dtypes': DTYPES_EVENT,
        'columns': list(DTYPES_EVENT.keys()),
        'default_column': list(DTYPES_EVENT.keys())[0],
        'included_columns': INCLUDED_COLUMNS_EVENT,
        'identifier_columns': ['event_id'],
        'date_column': 'date'
    },
    'VIOLATION_COUNTY': {
        'filename': 'VIOLATION_COUNTY',
        'dtypes': DTYPES_VIOLATION_CODE,
        'columns': list(DTYPES_VIOLATION_CODE.keys()),
        'default_column': list(DTYPES_VIOLATION_CODE.keys())[0],
        'included_columns': list(DTYPES_VIOLATION_CODE.keys()),
        'identifier_columns': [list(DTYPES_VIOLATION_CODE.keys())[0]],
        'date_column': None,
    },
    TABLE_NAME_REGISTERED_VEHICLES: {
        'filename': f'{TABLE_NAME_REGISTERED_VEHICLES}',
        'dtypes': DTYPES_REGISTERED_VEHICLES,
        'columns': list(DTYPES_REGISTERED_VEHICLES.keys()),
        'default_column': list(DTYPES_REGISTERED_VEHICLES.keys())[1],
        'included_columns': INCLUDED_COLUMNS_REGISTERED_VEHICLES,
        'identifier_columns': ['VIN'],
        'date_column': None
    },
    TABLE_NAME_PERMITTED_EVENTS: {
        'filename': f'{TABLE_NAME_PERMITTED_EVENTS}',
        'dtypes': DTYPES_PERMITTED_EVENTS,
        'columns': list(DTYPES_PERMITTED_EVENTS.keys()),
        'default_column': list(DTYPES_PERMITTED_EVENTS.keys())[1],
        'included_columns': INCLUDED_COLUMNS_PERMITTED_EVENTS,
        'identifier_columns': ['Event ID'],
        'date_column': 'Date'
    },
    TABLE_NAME_SCHOOLS: {
        'filename': f'{TABLE_NAME_SCHOOLS}',
        'dtypes': DTYPES_SCHOOLS,
        'columns': list(DTYPES_SCHOOLS.keys()),
        'default_column': list(DTYPES_SCHOOLS.keys())[1],
        'included_columns': INCLUDED_COLUMNS_SCHOOLS,
        'identifier_columns': ['BEDS'],
        'date_column': None
    },
    TABLE_NAME_TRAFFIC: {
        'filename': f'{TABLE_NAME_TRAFFIC}',
        'dtypes': DTYPES_TRAFFIC,
        'columns': list(DTYPES_TRAFFIC.keys()),
        'default_column': list(DTYPES_TRAFFIC.keys())[1],
        'included_columns': INCLUDED_COLUMNS_TRAFFIC,
        'identifier_columns': ['Plaza ID', 'Date', 'Hour', 'Direction'],
        'date_column': None
    }
}


def extract(table_name: str,
            data_path: str) -> dd.DataFrame:
    if table_name == 'PARKING_VIOLATION_ISSUED':
        list_data_filename = glob.glob(data_path + '/*.csv')
        data = dd.read_csv(urlpath=list_data_filename,
                           dtype=DATA_METADATA[table_name]['dtypes'],
                           blocksize='64MB')
    else:
        data = dd.read_csv(urlpath=f"{data_path}/{DATA_METADATA[table_name]['filename']}.csv",
                           dtype=DATA_METADATA[table_name]['dtypes'],
                           blocksize='64MB')
    return data
