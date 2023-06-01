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
from util.custom.registered_vehicles import TABLE_NAME_REGISTERED_VEHICLES, DTYPES_REGISTERED_VEHICLES, \
    INCLUDED_COLUMNS_REGISTERED_VEHICLES
from util.custom.weather import TABLE_NAME_WEATHER, DTYPES_WEATHER, INCLUDED_COLUMNS_WEATHER

DTYPES_VIOLATION_CODE = {
    'Violation County Code': 'str',
    'Violation County Name': 'str',
    'Violation County File Name Suffix': 'str'
}

DATA_FOLDER = 'custom/csv'

DATA_METADATA = {
    TABLE_NAME_LEGALLY_OPERATING_BUSINESS: {
        'filepath': f'{DATA_FOLDER}/{TABLE_NAME_LEGALLY_OPERATING_BUSINESS}',
        'dtypes': DTYPES_LEGALLY_OPERATING_BUSINESS,
        'columns': list(DTYPES_LEGALLY_OPERATING_BUSINESS.keys()),
        'default_column': list(DTYPES_LEGALLY_OPERATING_BUSINESS.keys())[0],
        'included_columns': INCLUDED_COLUMNS_LEGALLY_OPERATING_BUSINESS,
        'identifier_columns': ['DCA License Number', 'License Creation Date'],
        'date_column': 'License Creation Date'
    },
    TABLE_NAME_PARKING_VIOLATION_ISSUED: {
        'filepath': f'{DATA_FOLDER}/{TABLE_NAME_PARKING_VIOLATION_ISSUED}',
        'dtypes': DTYPES_PARKING_VIOLATION_ISSUED,
        'columns': list(DTYPES_PARKING_VIOLATION_ISSUED.keys()),
        'default_column': list(DTYPES_PARKING_VIOLATION_ISSUED.keys())[0],
        'included_columns': INCLUDED_COLUMNS_PARKING_VIOLATION_ISSUED,
        'identifier_columns': ['Summons Number', 'Issue Date'],
        'date_column': 'Issue Date'
    },
    TABLE_NAME_WEATHER: {
        'filepath': f'{DATA_FOLDER}/{TABLE_NAME_WEATHER}',
        'dtypes': DTYPES_WEATHER,
        'columns': list(DTYPES_WEATHER.keys()),
        'default_column': list(DTYPES_WEATHER.keys())[0],
        'included_columns': INCLUDED_COLUMNS_WEATHER,
        'identifier_columns': ['Time', 'Borough'],
        'date_column': 'Time'
    },
    TABLE_NAME_EVENT: {
        'filepath': f'{DATA_FOLDER}/{TABLE_NAME_EVENT}',
        'dtypes': DTYPES_EVENT,
        'columns': list(DTYPES_EVENT.keys()),
        'default_column': list(DTYPES_EVENT.keys())[0],
        'included_columns': INCLUDED_COLUMNS_EVENT,
        'identifier_columns': ['event_id'],
        'date_column': 'date'
    },
    'VIOLATION_COUNTY': {
        'filepath': f'{DATA_FOLDER}/VIOLATION_COUNTY.csv',
    },
    TABLE_NAME_REGISTERED_VEHICLES: {
        'filepath': f'{DATA_FOLDER}/{TABLE_NAME_REGISTERED_VEHICLES}',
        'dtypes': DTYPES_REGISTERED_VEHICLES,
        'columns': list(DTYPES_REGISTERED_VEHICLES.keys()),
        'default_column': list(DTYPES_REGISTERED_VEHICLES.keys())[1],
        'included_columns': INCLUDED_COLUMNS_REGISTERED_VEHICLES,
        'identifier_columns': ['VIN'],
        'date_column': None
    }
}


def extract(table_name: str,
            content_root_path='..') -> dd.DataFrame:
    data = dd.read_csv(urlpath=f"{content_root_path}/{DATA_METADATA[table_name]['filepath']}.csv",
                       dtype=DATA_METADATA[table_name]['dtypes'],
                       blocksize='64MB')
    return data
