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
from util.eda.eda import generate_analysis_plots



if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='Data analysis master job',
        description='This script generates postprocessing insights into the augmented data.')

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

    with Client(cluster, timeout='120s') as client:
        print('Dask client successfully initialized')

        with performance_report(filename=f"assets/Performance Reports/EDA-Performance-Report.html"):
            generate_analysis_plots(analysis_dir='./assets',
                                    data_path=config['environment'][environment_name]['data_output_dir'],
                                    content_root_path='' if environment_name == 'hpc' else './')
