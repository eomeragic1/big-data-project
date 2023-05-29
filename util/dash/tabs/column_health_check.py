import dash_bootstrap_components as dbc
from dash import html, dcc

from util.etl.extract import DATA_METADATA


def content_column_health_check():
    default_dataset_key = list(DATA_METADATA.keys())[0]

    return html.Div(children=[
        dbc.Row(children=[
            dbc.Col(children=[html.Label('Table Name: '),
                              dcc.Dropdown(options=list(DATA_METADATA.keys()),
                                           value=default_dataset_key,
                                           id='dropdown-column-table')]),
            dbc.Col(children=[html.Label('Column Name: '),
                              dcc.Dropdown(options=list(DATA_METADATA[default_dataset_key]['columns']),
                                           value=DATA_METADATA[default_dataset_key]['default_column'],
                                           id='dropdown-column')]),
            dbc.Col(),
            dbc.Col()
        ])
    ])


def _update_output_column_health_check(table_name: str,
                                       column_name: str):
    return html.Div()
