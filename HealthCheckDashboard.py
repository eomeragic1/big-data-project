import dash_bootstrap_components as dbc
from dash import Dash, dcc, html
from dash.dependencies import Input, Output

from util.dash.tabs.column_health_check import content_column_health_check, _update_output_column_health_check
from util.dash.tabs.table_health_check import content_table_health_check, _update_output_table_health_check

external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    {
        'href': 'https://use.fontawesome.com/releases/v5.15.3/css/all.css',
        'rel': 'stylesheet',
        'integrity': 'sha384-SZXxX4whJ79/gErwcOYf+zWLeJdY/qpuqC4cAa9rOGUstPomtqpuNWT9wdPEn2fk',
        'crossorigin': 'anonymous'
    }
]

app = Dash(__name__,
           external_stylesheets=external_stylesheets,
           suppress_callback_exceptions=True)

app.layout = html.Div([
    html.H1('New York Parking Violations: Data Health Check'),
    dcc.Tabs(id="tabs-input", value='tab-column-health-check', children=[
        dcc.Tab(label='Table Health Check', value='tab-table-health-check'),
        dcc.Tab(label='Column Health Check', value='tab-column-health-check')
    ]),
    html.Div(id='tab-content')
])


@app.callback(Output('tab-content', 'children'),
              Input('tabs-input', 'value'))
def render_content(tab_name: str):
    if tab_name == 'tab-table-health-check':
        return content_table_health_check()
    elif tab_name == 'tab-column-health-check':
        return content_column_health_check()


@app.callback(
    Output('output-table-health-check', 'children'),
    Input('dropdown-table', 'value')
)
def update_output_table_health_check(table_name: str):
    return _update_output_table_health_check(table_name=table_name)


@app.callback(
    Output('output-column-health-check', 'children'),
    Input('dropdown-column-table', 'value'),
    Input('dropdown-column', 'value')
)
def update_output_column_health_check(table_name: str,
                                      column_name: str):
    return _update_output_column_health_check(table_name=table_name,
                                              column_name=column_name)


if __name__ == '__main__':
    app.run_server(debug=True)
