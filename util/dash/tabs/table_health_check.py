import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from dash.html import Figure

from util.etl.initial.extract import DATA_METADATA


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
    fig = px.bar(data_frame=data.sort_values(by='Nullness', ascending=True),
                 x='Nullness',
                 y='Column Name',
                 title='Table Nullness')
    return fig


def viz_count_by_date(data: pd.DataFrame) -> Figure:
    fig = px.bar(data_frame=data.sort_values(by='Count', ascending=True),
                 x='Date',
                 y='Count',
                 title='Table Count By Date')
    return fig


def viz_table_data(data: pd.DataFrame):
    fig_time = px.bar(data_frame=data.sort_values(by='Execution time', ascending=True),
                 x='Processing mode',
                 y='Execution time',
                 color='File mode',
                 title='Table Execution Time')

    fig_memory = px.bar(data_frame=data.sort_values(by='Execution time', ascending=True),
                 x='Processing mode',
                 y='Peak memory usage',
                 color='File mode',
                 title='Table Memory Usage')
    return [
        dbc.Col(dcc.Graph(figure=fig_memory)),
        dbc.Col(dcc.Graph(figure=fig_time))
            ]


def _update_output_table_health_check(table_name: str):
    data_table_health = pd.read_parquet(f'data/parquet/PROCESSED_TABLE_METADATA.parquet')
    data_table_health = data_table_health[data_table_health['Table'] == table_name]

    data_nullness = pd.read_parquet(f'data/parquet/PROCESSED_COLUMN_NULLNESS.parquet')
    data_nullness = data_nullness[data_nullness['Table Name'] == table_name]

    data_count_row_by_date = pd.read_parquet(f'data/parquet/PROCESSED_COUNT_BY_DATE.parquet')
    data_count_row_by_date = data_count_row_by_date[data_count_row_by_date['Table Name'] == table_name]

    return html.Div(children=[
        dbc.Row(children=viz_table_data(data=data_table_health)),
        dbc.Row(children=[
            dbc.Col(dcc.Graph(figure=viz_non_nullness(data=data_nullness))),
            dbc.Col(dcc.Graph(figure=viz_count_by_date(data=data_count_row_by_date)))
        ])
    ])
