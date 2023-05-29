import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from dash.html import Figure
from dask import dataframe as dd

from util.data.common import read_parquet_table
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


def viz_non_nullness(data: dd.DataFrame) -> Figure:
    df_non_nullness = pd.Series(
        data=100 * data.count().compute() / len(data)
    ).reset_index()
    df_non_nullness.columns = ['Column Name', 'Non-Nullness [%]']

    fig = px.bar(data_frame=df_non_nullness.sort_values(by='Non-Nullness [%]', ascending=True),
                 x='Non-Nullness [%]',
                 y='Column Name',
                 title='Table Completeness')
    return fig


def viz_memory_usage(data: dd.DataFrame) -> Figure:
    df_memory_usage = pd.Series(
        data=data.memory_usage(deep=True).compute() / 1000000
    ).reset_index()
    df_memory_usage.columns = ['Column Name', 'Memory Usage [MB]']

    fig = px.bar(data_frame=df_memory_usage.sort_values(by='Memory Usage [MB]', ascending=True),
                 x='Memory Usage [MB]',
                 y='Column Name',
                 title='Table Memory Usage')
    return fig


def viz_table_data(data: dd.DataFrame,
                   table_name: str):
    date_column = DATA_METADATA[table_name]['date_column']
    id_column = DATA_METADATA[table_name]['date_column']

    len_data = len(data)
    last_date = dd.to_datetime(data[date_column]).max().compute().strftime('%d-%m-%Y')
    is_id_column_unique = len(set(data[id_column].unique().compute()))

    fig = go.Figure(data=[go.Table(header=dict(values=['Table Size', 'Freshness [Last Date]', 'Uniqueness']),
                                   cells=dict(values=[[len_data], [last_date], [is_id_column_unique]],
                                              height=30))
                          ])
    fig.update_layout(height=250)
    fig.update_traces(cells_font=dict(size=20))
    return fig


def _update_output_table_health_check(table_name: str):
    data = read_parquet_table(table_name=table_name, content_root_path='.')

    return html.Div(children=[
        dbc.Row(dcc.Graph(figure=viz_table_data(data=data, table_name=table_name))),
        dbc.Row(children=[
            dbc.Col(dcc.Graph(figure=viz_non_nullness(data=data))),
            dbc.Col(dcc.Graph(figure=viz_memory_usage(data=data)))
        ])])
