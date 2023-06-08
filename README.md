# Big Data Project :floppy_disk:

*Description*: This repository contains the code for the Big Data course at DataScience@UL-FRI. In this repository,
focus on processing big data with the help of a HPC cluster. We analyze the New York parking violations in the period between 2014 and 2023.

# Repo structure :blue_book:

This repository contains folders:

* ```assets/``` - contains the analysis plots and images that are used in documentation. It also includes performance reports for the ETL processes.
* ```data/``` - contains data placeholder directories.
* ```docs/``` - contains additional documentation of the repository.
* ```src/``` - contains the main logic of all data pipelines.
* ```util/``` - contains Python helper files that are used when running the scripts in `src/prod/*`.

# Computer specifications :computer:

Here are the specifications of the computer that was used to reproduce the original article:

- PyCharm Professional
- Python 3.8.16
- Windows 11 Pro
- Processor: 11th Gen Intel(R) Core(TM) i7-1165G7, 2.80GHz
- RAM: 32GB

# Reproducing results :snake:

To reproduce results, you will need to fork this repository and install Python dependencies using `conda`
and `pip`.

We used PyCharm to create an Anaconda environment. Alternatively, find instructions to do it via your
terminal [here](hhttps://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html).

Next, use `pip` to install requirements from `requirements.txt`:

```
pip install -r requirements.txt
```


Now your environment is ready to go. :partying_face: :clinking_glasses:


# Repo utilization
To run the code on an HPC cluster or to create an environment for streaming data, please follow the additional documentation:
- [Scaling Data Processing](docs/Scaling%20Data%20Processing.md)
- [Scaling Streaming Data](docs/Scaling%20Streaming%20Data.md)

To run the Plotly dashboard run:
```shell
python Dashboard.py
```