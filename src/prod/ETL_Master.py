import argparse
import inspect
import os
import sys

from box import Box

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
parent_parent_dir = os.path.dirname(parent_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, parent_parent_dir)

from util.custom.common import get_client
from util.etl.etl import etl_single_table_transformations, etl_augmentation

LIST_TABLE_NAME = [
    'LEGALLY_OPERATING_BUSINESS',
    'PARKING_VIOLATION_ISSUED',
    'WEATHER',
    'EVENT'
]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='ProgramName',
        description='What the program does',
        epilog='Text at the bottom of help')
    arg_data = parser.add_argument("-d", "--data",
                                   type=str,
                                   help="Specify dataset to process. If parameter is set to 'All' "
                                        "or not specified, all the possible datasets are processed."
                                        f"Possible values: {', '.join(LIST_TABLE_NAME)}.",
                                   default='All')
    arg_augmentation = parser.add_argument("-a",
                                           "--augmentation",
                                           help="Run data augmentation.",
                                           action='store_true',
                                           default=False)
    args = parser.parse_args()

    # Read configuration file
    config = Box.from_yaml(filename='config/config.yaml')
    # Initialize Dask cluster client:
    #   - connects to SLURM if configuration is set to 'hpc'
    print('Initializing Dask client...')
    client = get_client(config=config)
    print('Dask client successfully initialized')

    environment_name = config['environment']['name']

    # Perform the initial data transformations
    if args.data == 'All':
        etl_single_table_transformations(
            list_table_name=LIST_TABLE_NAME,
            input_data_path=config['environment'][environment_name]['data_augmentation_dir'],
            output_data_path=config['environment'][environment_name]['data_output_dir']
        )
    elif args.data in LIST_TABLE_NAME:
        list_table_name = [args.data]
        etl_single_table_transformations(
            list_table_name=list_table_name,
            input_data_path=config['environment'][environment_name]['data_augmentation_dir'],
            output_data_path=config['environment'][environment_name]['data_output_dir']
        )
    else:
        raise argparse.ArgumentError(argument=arg_data,
                                     message='Invalid argument value for data. Add the "-h" '
                                             'option to see argument specifications.')

    # Perform data augmentation on NY tickets dataset
    if args.augmentation:
        etl_augmentation(list_table_name=list(filter(lambda x: x != 'PARKING_VIOLATION_ISSUED', LIST_TABLE_NAME)))

    client.close()
