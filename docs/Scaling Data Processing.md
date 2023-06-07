# Scaling Data Processing

In this document, we explain how to scale the data processing procedure from local environment to high-performance
cluster.
We use the SLING HPC cluster. In order to scale the data processing, we setup a configuration file
in `config/config.yaml` that holds information about cluster settings and environment settings. Here is an example of
the configuration file:

```yaml
cluster:
  local:
    n_workers: 2
    threads_per_worker: 4
  hpc:
    memory: 16GB
    cores: 16
    processes: 8

environment:
  local:
    data_augmentation_dir: data/csv/augmentation
    data_original_dir: data/csv/original
    data_output_dir: data
  hpc:
    data_augmentation_dir: /d/hpc/home/lz3057/data/augmentation
    data_original_dir: /d/hpc/projects/FRI/bigdata/data/NYTickets
    data_output_dir: /d/hpc/home/lz3057/data/processed
```

The sample YAML file, specifies the cluster settings in local and HPC environment and also both the input and output
data paths.

## Configuration

**Cluster settings:**

- `local`: Local cluster settings.
    - `n_workers`: Number of workers to start.
    - `threads_per_worker`: Number of threads per each worker.
- `hpc`: SLURM HPC cluster settings.
    - `memory`: Total amount of memory per job.
    - `processes`: Cut the job up into this many processes. Good for GIL workloads or for nodes with many cores. By
      default, process ~= sqrt(cores) so that the number of processes and the number of threads per process is roughly
      the same.
    - `cores`: Total number of cores per job.

**Environment settings:**

- `data_augmentation_dir`: Path to the directory, where we dump the datasets that are used to augment the original
  dataset.
- `data_original_dir`: Path to the directory of the original/main dataset: New York Parking Violations Issued (this is a
  separate folder due to cluster setup, the data is pre-loaded there in a pre-specified folder).
- `data_output_dir`: Path to the directory where all the processed data will be dumped.

## Local environment

In order to run the ETL job in local environment, you simply execute the Python script.

```shell
python src/prod/ETL_Master.py 
```

**Optional arguments to the Python script:**

- `-h`: Lists hints to help setup the Python script arguments correctly.
- `-e`/`--env`: Specifies the environment name. On SLURM HPC cluster, choose *hpc*, otherwise choose *local*.
- `-d`/`--data`: Specifies the dataset name to be processed. Use the `-h` flag to see all the available options.
- `-a`/`--augmentation`: Flag to conclude the ETL job with the augmentation process of the original dataset.

### Copying augmentation data

From the local environment, you must also copy the augmentation datasets, i.e. the data we wish to use to augment the
original dataset. This can be done by running a simple Python script:

```shell
python src/prod/SCP_DataCSV
```

Pre-requisites:

- Setup `config/secrets.yaml` configuration with SSH connection settings, for example:

```
slurm_cluster:
  hostname: hpc-login.arnes.si
  username: <USERNAME>
  password: <PASSWORD>
```

### Saving `requirements.txt`

To be able to reproduce the environment on remote HPC cluster, we must save the environmental dependencies.

```shell
pip list --format=freeze  > .\requirements.txt
```

Or alternatively:

```shell
pip install pipreqs
pipreqs . --force
```

## HPC environment

In order to scale the data processing procedure on a remote, HPC cluster, we must first setup the environment and then
run the Python script with `srun`.

### Setup environment

To setup the environment, we clone the GitHub repository. Since it is a private repository, we use an authentication
token.

```shell
git clone https://<USERNAME>:<TOKEN>@github.com/eomeragic1/big-data-project.git
```

We create a new Anaconda environment with Python version 3.8.16.

```shell
conda create --name bigdata python=3.8.16
 ```

We activate the new Anaconda environment by running:

```shell
conda activate bigdata
```

We install the required dependencies.

```shell
pip install -r requirements.txt
```

We run the data processing procedure with `--env hpc` setting and any additional settings we wish to include (see above
for all possible options).
If we wish to change the scaling settings, we can edit the `config/config.yaml` file.

```shell
srun python src/prod/ETL_Master.py --env hpc <ADDITIONAL_SETTINGS>
```

### Connecting to Dask dashboard via SSH tunneling

To connect to the Dask dashboard on remote cluster, we must first get the link from the dashboard, which is printed while running the ETL Python script.

![Dashboard Link.png](../assets/Dashboard%20Link.png)

We activate SSH tunneling by running an additional SSH connection with the following command:

```shell
 ssh -L 8000:<REMOTE_DASK_DASHBOARD_LINK> <SLURM_CLUSTER_USERNAME>@hpc-login.arnes.si
```

In the image above, we would replace the variable **<REMOTE_DASK_DASHBOARD_LINK>** with `153.5.72.117:8787`.

The remote Dask dashboard is now running locally on [localhost:8000](localhost:8000).