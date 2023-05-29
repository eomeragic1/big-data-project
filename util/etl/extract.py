import dask.dataframe as dd

from util.data.legally_operating_business import \
    DTYPES_LEGALLY_OPERATING_BUSINESS, \
    TABLE_NAME_LEGALLY_OPERATING_BUSINESS, \
    INCLUDED_COLUMNS_LEGALLY_OPERATING_BUSINESS
from util.data.parking_violation_issued import \
    DTYPES_PARKING_VIOLATION_ISSUED, \
    TABLE_NAME_PARKING_VIOLATION_ISSUED, \
    INCLUDED_COLUMNS_PARKING_VIOLATION_ISSUED

DTYPES_VIOLATION_CODE = {
    'Violation County Code': 'str',
    'Violation County Name': 'str',
    'Violation County File Name Suffix': 'str'
}

DATA_FOLDER = 'data/csv'

DATA_METADATA = {
    TABLE_NAME_LEGALLY_OPERATING_BUSINESS: {
        'filepath': f'{DATA_FOLDER}/{TABLE_NAME_LEGALLY_OPERATING_BUSINESS}',
        'dtypes': DTYPES_LEGALLY_OPERATING_BUSINESS,
        'columns': list(DTYPES_LEGALLY_OPERATING_BUSINESS.keys()),
        'default_column': list(DTYPES_LEGALLY_OPERATING_BUSINESS.keys())[0],
        'included_columns': INCLUDED_COLUMNS_LEGALLY_OPERATING_BUSINESS,
        'identifier_column': 'DCA License Number',
        'date_column': 'License Creation Date'
    },
    TABLE_NAME_PARKING_VIOLATION_ISSUED: {
        'filepath': f'{DATA_FOLDER}/{TABLE_NAME_PARKING_VIOLATION_ISSUED}',
        'dtypes': DTYPES_PARKING_VIOLATION_ISSUED,
        'columns': list(DTYPES_PARKING_VIOLATION_ISSUED.keys()),
        'default_column': list(DTYPES_PARKING_VIOLATION_ISSUED.keys())[0],
        'included_columns': INCLUDED_COLUMNS_PARKING_VIOLATION_ISSUED,
        'identifier_column': 'Summons Number',
        'date_column': 'Issue Date'
    }
}


def extract(table_name: str,
            content_root_path='..') -> dd.DataFrame:
    data = dd.read_csv(urlpath=f"{content_root_path}/{DATA_METADATA[table_name]['filepath']}.csv",
                       dtype=DATA_METADATA[table_name]['dtypes'],
                       blocksize='64MB')
    return data
