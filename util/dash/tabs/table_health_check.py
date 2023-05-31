import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from dash.html import Figure
from dask import dataframe as dd

from util.etl.extract import DATA_METADATA


def content_table_health_check():
    return html.Div(children=[
        dbc.Row(children=[
            dbc.Col(children=[html.Label('Table Name: '),
                              dcc.Dropdown(list(DATA_METADATA.keys()), list(DATA_METADATA.keys())[0],
                                           id='dropdown-table')]),
            dbc.Col(),
            dbc.Col(),
            dbc.Col()
        ]),
        html.Div(id='output-table-health-check')
    ])


def viz_non_nullness(data: pd.DataFrame) -> Figure:
    fig = px.bar(data_frame=data.sort_values(by='Non-Nullness [%]', ascending=True),
                 x='Non-Nullness [%]',
                 y='Column Name',
                 title='Table Completeness')
    return fig


def viz_memory_usage(data: pd.DataFrame) -> Figure:
    fig = px.bar(data_frame=data.sort_values(by='Memory Usage [MB]', ascending=True),
                 x='Memory Usage [MB]',
                 y='Column Name',
                 title='Table Memory Usage')
    return fig


def viz_table_data(data: pd.DataFrame):
    len_data = format(data.values[0][1], ',')
    last_date = data.values[0][2].strftime('%Y-%m-%d')
    unique_rows = round(data.values[0][3], 2)
    fig = go.Figure(data=[go.Table(header=dict(values=['Table Size', 'Freshness [Last Date]', 'Uniqueness [%]']),
                                   cells=dict(values=[[len_data], [last_date], [unique_rows]],
                                              height=30))
                          ])
    fig.update_layout(height=250)
    fig.update_traces(cells_font=dict(size=20))
    return fig


def _update_output_table_health_check(table_name: str):
    data_table_health = pd.read_parquet(f'data/parquet/PROCESSED_TABLE_HEALTH.parquet')
    data_table_health = data_table_health[data_table_health['Table Name'] == table_name]

    data_column_health = pd.read_parquet(f'data/parquet/PROCESSED_COLUMN_HEALTH.parquet')
    data_column_health = data_column_health[data_column_health['Table Name'] == table_name]
    return html.Div(children=[
        dbc.Row(dcc.Graph(figure=viz_table_data(data=data_table_health))),
        dbc.Row(children=[
            dbc.Col(dcc.Graph(figure=viz_non_nullness(data=data_column_health))),
            dbc.Col(dcc.Graph(figure=viz_memory_usage(data=data_column_health)))
        ])])
