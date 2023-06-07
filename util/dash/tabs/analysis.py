import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

import plotly.express as px
import numpy as np
import datetime
import dask.dataframe as dd

from util.custom.common import read_parquet_table
from util.etl.initial.table.parking_violation_issued import TABLE_NAME_PARKING_VIOLATION_ISSUED


def content_analysis():
    tabs_options = ['Avg No. Tickets / Hour / Borough',
                    'Avg No. Tickets / Day / Borough',
                    'Avg No. Tickets / Weekday / Borough',
                    'Sum of No. Tickets / Day / Vehicle Make',
                    'Sum of Fines Paid in $ / Day / Vehicle Make',
                    'No. Tickets / Day / Driver US State']
    return html.Div(children=[
        dbc.Row(children=[html.Label('Figure Name: '),
                          dcc.Dropdown(options=tabs_options,
                                       value=tabs_options[0],
                                       id='dropdown-analysis')
                          ]),
        html.Div(id='output-analysis-graphs')
    ])


def _update_output_analysis_graph(graph_name: str):
    return html.Div(children=[dbc.Row(children=viz_graph(graph_name=graph_name), justify="center")])


def viz_graph(graph_name: str):
    if graph_name == 'Avg No. Tickets / Hour / Borough':
        return html.Img(src=dash.get_asset_url('../../../assets/tickets_hour_borough.png'), height=600, style={'width': '60%'})
    elif graph_name == 'Avg No. Tickets / Day / Borough':
        return html.Img(src=dash.get_asset_url('../../../assets/tickets_day_borough.png'), height=600, style={'width': '60%'})
    elif graph_name == 'Avg No. Tickets / Weekday / Borough':
        return html.Img(src=dash.get_asset_url('../../../assets/tickets_weekday_borough.png'), height=600, style={'width': '60%'})
    elif graph_name == 'Sum of No. Tickets / Day / Vehicle Make':
        return html.Video(src=dash.get_asset_url('../../../assets/vehicle_makers.mp4'), controls=True, height=600, style={'width': '60%'})
    elif graph_name == 'Sum of Fines Paid in $ / Day / Vehicle Make':
        return html.Video(src=dash.get_asset_url('../../../assets/vehicle_makers_fines.mp4'), controls=True, height=600, style={'width': '60%'})
    elif graph_name == 'No. Tickets / Day / Driver US State':
        return dcc.Graph(figure=viz_map_of_tickets())


def viz_map_of_tickets():
    data = read_parquet_table(table_name=TABLE_NAME_PARKING_VIOLATION_ISSUED, content_root_path='.')
    data['Issue Date'] = dd.to_datetime(data['Issue Date'])
    grouped_df = data.groupby(['Registration State', 'Issue Date']).size().reset_index()
    grouped_df = grouped_df.loc[(grouped_df['Issue Date'] > datetime.datetime(2022, 6, 1)) & (
                grouped_df['Issue Date'] < datetime.datetime(2023, 5, 15)), :].rename(
        columns={0: 'Count'}).compute().sort_values(by=['Issue Date', 'Registration State'])
    grouped_df['Count'] = grouped_df['Count'] + 1
    fig = px.choropleth(grouped_df,
                        locations='Registration State',
                        locationmode="USA-states",
                        scope="usa",
                        color=np.log10(grouped_df['Count']),
                        color_continuous_scale="Viridis_r",
                        animation_frame='Issue Date',
                        )
    fig.update_layout(height=600, width=600)
    return fig

