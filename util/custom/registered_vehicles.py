TABLE_NAME_REGISTERED_VEHICLES = f'REGISTERED_VEHICLES'

INCLUDED_COLUMNS_REGISTERED_VEHICLES = [
    'Record Type',
    'VIN',
    'City',
    'State',
    'Zip',
    'Make',
    'Fuel Type',
    'Reg Valid Date',
    'Reg Expiration Date',
]
DTYPES_REGISTERED_VEHICLES = {
    'Record Type': 'str',
    'VIN': 'str',
    'Registration Class': 'str',
    'City': 'str',
    'State': 'str',
    'Zip': 'int',
    'County': 'str',
    'Model Year': 'int',
    'Make': 'str',
    'Body Type': 'str',
    'Fuel Type': 'str',
    'Unladen Weight': 'str',
    'Maximum Gross Weight': 'str',
    'Passengers': 'str',
    'Reg Valid Date': 'str',
    'Reg Expiration Date': 'str',
    'Color': 'str',
    'Scofflaw Indicator': 'str',
    'Suspension Indicator': 'str',
    'Revocation Indicator': 'str'
}
