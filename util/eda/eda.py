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

    plt.savefig(analysis_dir + '/plot_1.png', facecolor='white', bbox_inches='tight')
    print('Finished generating first plot')
    ## Second plot - Timeline chart, amount of tickets per day for each borough

    violations_per_day = augmented_data[['Issue Date', 'Violation County Name']]
    violations_per_day = violations_per_day.groupby([violations_per_day['Issue Date'].dt.year.rename('Year'),
                                                     violations_per_day['Issue Date'].dt.month.rename('Month'),
                                                     'Violation County Name']).size().reset_index().compute()
    violations_per_day.columns = ['Year', 'Month', 'Violation County', 'Counts']
    violations_per_day['Issue Date'] = pd.to_datetime(
        violations_per_day['Year'].astype(str) + '-' + violations_per_day['Month'].astype(str) + '-1')
    violations_per_day_pivot = violations_per_day[['Issue Date', 'Violation County', 'Counts']].pivot(
        index='Issue Date', columns='Violation County', values='Counts')

    population_density = {'Manhattan': 28872,
                          'Brooklyn': 15227,
                          'Bronx': 13482,
                          'Queens': 8542,
                          'Staten Island': 3327}
    for column in violations_per_day_pivot.columns:
        violations_per_day_pivot[column] = violations_per_day_pivot[column] / population_density[column]

    fig, ax = plt.subplots()
    ax.plot(violations_per_day_pivot)
    ax.set_xlabel('Hour')
    ax.set_ylabel('Number of tickets / (people / sq. km)')
    ax.legend(violations_per_day_pivot.columns)
    ax.grid(0.6)
    ax.set_title(
        'Daily county of tickets for each borough through time \n normalized by population density (people/sq. km)')
    plt.savefig(analysis_dir + '/plot_2.png', facecolor='white', bbox_inches='tight')
    print('Finished generating second plot')

    ## Third plot - Average amount of tickets during each day of the week for each borough

    augmented_data['Weekday'] = augmented_data['Issue Date'].apply(lambda x: pd.to_datetime(x, unit='s').dayofweek,
                                                                   meta=('Issue Weekday', int))
    violations_per_weekday = augmented_data[['Weekday', 'Violation County Name']].groupby(
        ['Weekday', 'Violation County Name']).size().reset_index().compute()
    violations_per_weekday.columns = ['Weekday', 'Violation County', 'Counts']
    violations_per_weekday['Counts'] = violations_per_weekday['Counts'] / no_days
    violations_per_weekday_pivot = violations_per_weekday.pivot(index='Weekday', columns='Violation County',
                                                                values='Counts').sort_index()
    violations_per_weekday_pivot.index = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    violations_per_weekday_pivot.plot(kind='bar', figsize=(10, 6))
    plt.xlabel('Weekday')
    plt.ylabel('Average no. Parking Violations')
    plt.title('Average no. Parking Violations by County and Weekday')
    plt.grid(0.2)
    plt.legend(loc='upper right')
    plt.savefig(analysis_dir + '/plot_3.png', facecolor='white', bbox_inches='tight')
    print('Finished generating third plot')

    ## Fourth and fifth plot - Car manufacturers
    vehicle_mappings = {'HONDA': 'Honda', 'TOYOT': 'Toyota', 'FORD': 'Ford', 'NISSA': 'Nissan', 'CHEVR': 'Chevrolet',
                        'ME/BE': 'Mercedes-Benz', 'BMW': 'BMW',
                        'JEEP': 'Jeep', 'HYUND': 'Hyundai', 'LEXUS': 'Lexus', 'ACURA': 'Acura', 'FRUEH': 'Freuheur',
                        'DODGE': 'Dodge', 'SUBAR': 'Subaru',
                        'KIA': 'Kia', 'VOLKS': 'Volkswagen', 'AUDI': 'Audi', 'MAZDA': 'Mazda', 'INFIN': 'Infiniti',
                        'RAM': 'RAM', 'GMC': 'GMC', 'ROVER': 'Rover',
                        'CHRYS': 'Chrysler', 'HIN': 'Hino', 'CADIL': 'Cadillac', 'VOLVO': 'Volvo',
                        'INTER': 'International Harvester', 'TESLA': 'Tesla', 'ISUZU': 'Isuzu', 'MITSU': 'Mitsubishi'}

    def apply_filter_vehicle_makers(x):
        if x not in vehicle_mappings.keys():
            return ''
        return x

    augmented_data['Vehicle Make'] = augmented_data['Vehicle Make'].apply(apply_filter_vehicle_makers,
                                                                          meta=('Vehicle Make', str)).replace(
        '', np.nan)
    vehicle_makers = augmented_data.dropna(subset='Vehicle Make').groupby(['Vehicle Make']).size().reset_index()
    vehicle_makers.columns = ['Vehicle Make', 'Counts']

    registered_vehicles = read_parquet_table(table_name='REGISTERED_VEHICLES',
                                             data_path=data_path + '/parquet',
                                             content_root_path=content_root_path).add_prefix('RV - ')
    vehicle_stats = dd.merge(
        left=vehicle_makers,
        right=registered_vehicles,
        left_on='Vehicle Make',
        right_on='RV - Vehicle Make',
        how='inner'
    )
    vehicle_stats['Ratio'] = vehicle_stats['Counts'] / vehicle_stats['RV - Count']
    vehicle_stats = vehicle_stats.nlargest(n=20, columns=['Ratio'])
    vehicle_stats['Vehicle Make'] = vehicle_stats['Vehicle Make'].apply(lambda x: vehicle_mappings[x],
                                                                        meta=('Vehicle Make', str))
    vehicle_stats = vehicle_stats.compute()

    fig, ax = plt.subplots()
    ax.barh(vehicle_stats.head(10)['Vehicle Make'], vehicle_stats.head(10)['Ratio'], color='orange')
    for i, count in enumerate(vehicle_stats.head(10)['Ratio']):
        ax.text(count, i, "{0:.2f}".format(count), ha='left', va='center')

    ax.set_title('Vehicle makes with the highest ratio of \n parking violations over the number of registered vehicles')
    ax.set_xlabel('Ratio')
    ax.set_ylabel('Vehicle Make')
    ax.grid(0.6)
    ax.invert_yaxis()
    plt.savefig(analysis_dir + '/plot_4.png', facecolor='white', bbox_inches='tight')
    print('Finished generating fourth plot')

    fig, ax = plt.subplots()
    ax.barh(vehicle_stats.iloc[3:13]['Vehicle Make'], vehicle_stats.iloc[3:13]['Ratio'], color='orange')
    for i, count in enumerate(vehicle_stats.iloc[3:13]['Ratio']):
        ax.text(count, i, "{0:.2f}".format(count), ha='left', va='center')

    ax.set_title('Vehicle makes with the highest ratio of \n parking violations over the number of registered vehicles')
    ax.set_xlabel('Ratio')
    ax.set_ylabel('Vehicle Make')
    ax.grid(0.6)
    ax.invert_yaxis()
    plt.savefig(analysis_dir + '/plot_5.png', facecolor='white', bbox_inches='tight')
    print('Finished generating fifth plot')

    ## Sixth plot - manufacturers and $$$

    fine_codes = read_parquet_table('PARKING_VIOLATION_CODES', data_path=data_path + '/parquet',
                                    content_root_path=content_root_path)
    augmented_data['Violation Code'] = augmented_data['Violation Code'].astype('int64')
    vehicles_makers_fines = \
        augmented_data.dropna(subset='Vehicle Make').merge(fine_codes, how="inner", on=["Violation Code"]).groupby(
            ['Vehicle Make'])['Fine Amount'].sum().reset_index().sort_values(['Fine Amount'], ascending=False)

    vehicles_makers_fines.columns = ['Vehicle Make', 'Fine Amount']
    vehicles_makers_fines['Vehicle Make'] = vehicles_makers_fines['Vehicle Make'].apply(lambda x: vehicle_mappings[x],
                                                                                        meta=('Vehicle Make', str))
    vehicles_makers_fines = vehicles_makers_fines.compute()
    fig, ax = plt.subplots()
    ax.barh(vehicles_makers_fines.head(10)['Vehicle Make'], vehicles_makers_fines.head(10)['Fine Amount'],
            color='orange')
    for i, count in enumerate(vehicles_makers_fines.head(10)['Fine Amount']):
        ax.text(count, i, "${0:.1f}M".format(count / 1000000.), ha='left', va='center')

    ax.set_title('Vehicle makes with the highest amount \n of fines paid')
    ax.set_xlabel('Fine Amount')
    ax.set_ylabel('Vehicle Make')
    ax.grid(0.6)
    ax.invert_yaxis()
    plt.savefig(analysis_dir + '/plot_6.png', facecolor='white', bbox_inches='tight')
    print('Finished generating sixth plot')

    ## Seventh plot - States that people come from

    grouped_df = augmented_data.dropna(subset=['Registration State']).groupby(
        ['Registration State']).size().reset_index().rename(columns={0: 'Count'}).sort_values(by=['Count']).compute()
    grouped_df['Count'] = np.log10(grouped_df['Count'])
    fig = px.choropleth(grouped_df,
                        locations='Registration State',
                        locationmode='USA-states',
                        scope="usa",
                        color=grouped_df['Count'],
                        color_continuous_scale="armyrose",
                        title='Number of Parking Violations per US State (Logarithmic Colorbar)',
                        labels={'Count': 'Number of Parking Violations'})
    colorbar_tickvals = [0, 1, 2, 3, 4, 5, 6, 7, 8]  # Example values, adjust as needed
    colorbar_ticktext = ['1', '10', '100', '1000', '10000', '100000', '1000000',
                         '10000000', '100000000']  # Example labels, adjust as needed
    fig.update_layout(coloraxis_colorbar=dict(
        tickmode='array',
        tickvals=colorbar_tickvals,
        ticktext=colorbar_ticktext,
    ))
    fig.write_image("assets/plot_7.png")
    print('Finished generating seventh plot')

    ## Ratio for weather

    temperature_data = augmented_data[['W - Temperature', 'Violation County Name']].dropna(subset=['W - Temperature'])
    temperature_data['W - Temperature'] = temperature_data['W - Temperature'].astype(float).apply(lambda x: round(x),
                                                                                                  meta=(
                                                                                                      'W - Temperature',
                                                                                                      int))
    temperature_data = temperature_data.groupby(
        ['W - Temperature', 'Violation County Name']).size().reset_index().rename(
        columns={0: 'Violations Temperature Count'})

    weather_data = read_parquet_table('WEATHER', data_path=data_path + '/parquet',
                                      content_root_path=content_root_path).dropna(subset=['Temperature'])
    weather_data['Temperature'] = weather_data['Temperature'].astype(float).apply(lambda x: round(x),
                                                                                  meta=('Temperature', int))
    weather_data = weather_data.groupby(['Temperature', 'Borough']).size().reset_index().rename(
        columns={0: 'Total Temperature Count'})

    merged_df = temperature_data.merge(weather_data, left_on=['W - Temperature', 'Violation County Name'],
                                       right_on=['Temperature', 'Borough'], how='inner')
    merged_df['Ratio'] = merged_df['Violations Temperature Count'] / merged_df['Total Temperature Count']

    merged_df = merged_df.sort_values(by=['Temperature', 'Borough']).compute()
    fig, ax = plt.subplots()
    for label, group in merged_df.groupby(['Borough']):
        group.plot(x='Temperature', y='Ratio', label=label, ax=ax)
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Ratio')
    plt.title(
        'Ratio of the amount of parking violations issued \n during each temperature range over \n the total amount of times that temperature range\n was present')
    plt.legend()
    plt.grid(0.6)
    plt.savefig(analysis_dir + '/plot_8.png', facecolor='white', bbox_inches='tight')
    print('Finished generating eight plot')
