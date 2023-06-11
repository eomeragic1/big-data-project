import argparse
import inspect
import os
import sys

from box import Box
from dask.distributed import Client
from distributed import performance_report

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
parent_parent_dir = os.path.dirname(parent_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, parent_parent_dir)

from util.custom.common import get_dask_cluster
from util.etl.jobs import etl_single_table_transformations, etl_augmentation

LIST_TABLE_NAME = [
    'LEGALLY_OPERATING_BUSINESS',
    'PARKING_VIOLATION_ISSUED',
    'VIOLATION_COUNTY',
    'PERMITTED_EVENTS',
    'SCHOOLS',
    'TRAFFIC',
    'REGISTERED_VEHICLES',
    'WEATHER',
    'PARKING_VIOLATION_CODES',
]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='ETL Master Job',
        description='This script processes and augments the New York parking violation tickets.')

    arg_data = parser.add_argument("-d", "--data",
                                   type=str,
                                   help=f"Specify dataset to process. If parameter is set to 'All' or not specified, all the possible datasets are processed. Possible values: {', '.join(LIST_TABLE_NAME)}.",
                                   default='None')
    arg_augmentation = parser.add_argument("-a",
                                           "--augmentation",
                                           help="Run data augmentation.",
                                           action='store_true',
                                           default=False)
    arg_env = parser.add_argument("-e",
                                  "--env",
                                  help="Specify environment: 'local'/'hpc'.",
                                  type=str,
                                  default='local')
    args = parser.parse_args()

    if args.env in ('local', 'hpc'):
        environment_name = args.env
    else:
        raise argparse.ArgumentError(argument=arg_env,
                                     message='Invalid argument value for environment. Add the "-h" option to see argument specifications.')

    # Read configuration file
    config = Box.from_yaml(filename='config/config.yaml')

    # Initialize Dask cluster client:
    #   - connects to SLURM if configuration is set to 'hpc'

    print('Initializing Dask client...')
    cluster = get_dask_cluster(config=config,
                               environment_name=environment_name)

    with Client(cluster,
                timeout='120s') as client:

        print(f'Dask client successfully initialized')
        print(f'Scheduler: {cluster.scheduler.address}')
        print(f'Cluster: {cluster.dashboard_link}')
        with performance_report(filename=f"assets/Performance Reports/Total-Performance-Report.html"):

            # Perform the initial data transformations
            if args.data == 'All':
                etl_single_table_transformations(
                    list_table_name=LIST_TABLE_NAME,
                    config=config,
                    environment_name=environment_name
                )
            elif args.data in LIST_TABLE_NAME:
                list_table_name = [args.data]
                etl_single_table_transformations(
                    list_table_name=list_table_name,
                    config=config,
                    environment_name=environment_name
                )
            else:
                raise argparse.ArgumentError(argument=arg_data,
                                             message='Invalid argument value for data. Add the "-h" option to see argument specifications.')

        # Perform data augmentation on NY tickets dataset
        if args.augmentation:
            with performance_report(filename=f"assets/Performance Reports/Augmentation-Performance-Report.html"):
                etl_augmentation(
                    list_table_name=list(filter(lambda x: x != 'PARKING_VIOLATION_ISSUED', LIST_TABLE_NAME)),
                    data_path=config['environment'][environment_name]['data_output_dir'],
                    content_root_path='' if environment_name == 'hpc' else './')
        print('Press enter to close the cluster...')
        input('')
