import os

from util.custom.common import read_parquet_table
import dask.dataframe as dd
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import plotly.express as px


def generate_analysis_plots(analysis_dir: str, data_path: str, content_root_path: str):
    plt.rcParams['axes.axisbelow'] = True
    augmented_data = read_parquet_table(table_name='AUGMENTED_PARKING_VIOLATION_ISSUED',
                                        data_path=data_path + '/parquet',
                                        content_root_path=content_root_path)
    augmented_data['Issue Date'] = dd.to_datetime(augmented_data['Issue Date'])

    # First plot - Counts/Mean chart per hour for each borough

    violations_per_hour = augmented_data[['Violation Time', 'Violation County Name']].groupby(
        ['Violation Time', 'Violation County Name']).size().reset_index().compute()
    no_days = len(augmented_data['Issue Date'].value_counts(sort=False).compute())
    violations_per_hour.columns = ['Violation Time', 'Violation County', 'Counts']
    violations_per_hour['Violation Time'] = violations_per_hour['Violation Time'].astype(int)
    violations_per_hour['Counts'] = violations_per_hour['Counts'] / no_days
    violations_per_hour_pivot = violations_per_hour.pivot(index='Violation Time', columns='Violation County',
                                                          values='Counts')

    print(os.getcwd())
    traffic = read_parquet_table(table_name='TRAFFIC', data_path=data_path + '/parquet',
                                 content_root_path=content_root_path).compute()
    traffic['Hour'] = traffic['Hour'].astype(int)
    traffic_pivot = traffic.pivot(index='Hour', columns='Direction', values='Average').sort_index()
    traffic_pivot.columns = ['Incoming', 'Outgoing']
    fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))

    ax[0].plot(violations_per_hour_pivot)
    ax[0].set_xlabel('Hour')
    ax[0].set_ylabel('Counts')
    ax[0].legend(violations_per_hour_pivot.columns)
    ax[0].grid(0.6)
    ax[0].set_title('Average amount of tickets by \n hour of day for different counties')

    ax[1].plot(traffic_pivot)
    ax[1].set_xlabel('Hour')
    ax[1].set_ylabel('Avg. Traffic Count')
    ax[1].legend(traffic_pivot.columns)
    ax[1].grid(0.6)
    ax[1].set_title('Average traffic count \n on New York City bridges and tunnels')

    ## Scatter plot for events count

    events_data = augmented_data.groupby(['Issue Date', 'Violation County Name']).agg(
        {'PE - Events Count': 'mean', 'PE - Event Borough': 'count'}).reset_index().compute()
    events_data.columns = ['Date', 'Borough', 'Events Count', 'Violations Count']
    fig, ax = plt.subplots()
    colors = ['blue', 'green', 'orange', 'red', 'black']
    for i, (label, group) in enumerate(events_data.groupby(['Borough'])):
        group.plot.scatter(x='Events Count', y='Violations Count', label=label, ax=ax, c=colors[i])
    plt.xlabel('Number of active events')
    plt.ylabel('Number of violations')
    plt.title('Comparison of the number of active permitted events in each borough\n to the number of violations that happened that day')
    plt.legend()
    plt.grid(0.6)
    plt.savefig(analysis_dir + '/plot_11.png', facecolor='white', bbox_inches='tight')
    print('Finished generating eleventh plot')

    ## Scatter plot for schools count
    school_data = augmented_data.dropna(subset=['S - Police Precinct']).groupby(['S - Police Precinct']).agg(
        {'S - Number of Schools': 'mean', 'Violation Precinct': 'count'}).reset_index().compute()
    school_data.columns = ['Precinct', 'Number of Schools', 'Number of violations']
    fig, ax = plt.subplots()
    z = np.polyfit(x=school_data['Number of Schools'], y=school_data['Number of violations'], deg=1)
    # Create sequence of 100 numbers from 0 to 100
    xseq = np.linspace(0, 80, num=10)
    ax.plot(xseq, z[1] + z[0] * xseq, color="k", lw=2.5)
    ax.scatter(school_data['Number of Schools'], school_data['Number of violations'], c='orange')
    plt.xlabel('Number of schools in the precinct')
    plt.ylabel('Number of violations')
    plt.title(
        'Comparison of the number of schools in each police precinct\n to the number of violations that happened in that police precinct')
    plt.legend()
    plt.grid(0.6)
    plt.savefig(analysis_dir + '/plot_12.png', facecolor='white', bbox_inches='tight')
    print('Finished generating 12th plot')
