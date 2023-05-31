import pandas as pd
from box import Box
from sodapy import Socrata

DATA_FOLDER = 'custom/csv'
DATA_FILEPATH_LEGALLY_OPERATING_BUSINESSES = f'{DATA_FOLDER}/LEGALLY_OPERATING_BUSINESS.csv'

config = Box.from_yaml(filename='config/config.yaml')

client = Socrata(domain=config['socrata']['domain'],
                 app_token=config['socrata']['app_token'],
                 username=config['socrata']['username'],
                 password=config['socrata']['password'],
                 timeout=10)

results = client.get(dataset_identifier=config['socrata']['dataset_id'],
                     limit=1000)

df_parking_violations = pd.DataFrame.from_records(results)
data_legally_operating_businesses = pd.read_csv(DATA_FILEPATH_LEGALLY_OPERATING_BUSINESSES)

set(df_parking_violations['street_name']).intersect(set(data_legally_operating_businesses['Address Street Name']))

set(df_parking_violations['violation_county'])
