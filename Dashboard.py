import dash_bootstrap_components as dbc
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots

from src.prod.streaming.plot_consumer import connect
from util.dash.tabs.analysis import content_analysis, _update_output_analysis_graph
from util.dash.tabs.data_augmentation_quality import content_data_augmentation_quality
from util.dash.tabs.streams import content_streams
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
    html.H1('New York Parking Violations: Dashboard'),
    dcc.Tabs(id="tabs-input", value='tab-table-health-check', children=[
        dcc.Tab(label='Table Health Check', value='tab-table-health-check'),
        dcc.Tab(label='Analysis', value='tab-analysis'),
        dcc.Tab(label='Streams', value='tab-streams'),
    ]),
    html.Div(id='tab-content')
])


@app.callback(Output('tab-content', 'children'),
              Input('tabs-input', 'value'))
def render_content(tab_name: str):
    if tab_name == 'tab-table-health-check':
        return content_table_health_check()
    elif tab_name == 'tab-analysis':
        return content_analysis()
    elif tab_name == 'tab-streams':
        return content_streams()


@app.callback(
    Output('output-table-health-check', 'children'),
    Input('dropdown-table', 'value')
)
def update_output_table_health_check(table_name: str):
    return _update_output_table_health_check(table_name=table_name)


@app.callback(
    Output('output-analysis-graphs', 'children'),
    Input('dropdown-analysis', 'value')
)
def update_output_analysis_graph(graph_name: str):
    return _update_output_analysis_graph(graph_name=graph_name)

@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):
    # Collect some data
    data = connect.get_graph_data()

    # Create the graph with subplots
    fig = make_subplots(rows=1, cols=2)
    fig['layout']['margin'] = {
        'l': 30, 'r': 10, 'b': 30, 't': 10
    }
    fig['layout']['legend'] = {'x': 0, 'y': 1, 'xanchor': 'left'}

    fig.add_trace({
        'x': data['Violation County'],
        'y': data['Temperature'],
        'name': 'Altitude',
        'type': 'box'
    }, 1, 1)

    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
