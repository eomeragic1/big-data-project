#https://dash.plotly.com/live-updates

from datetime import datetime

import dash
from dash import html, dcc
import plotly
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots

from util.kafka.consumer import KafkaConnect


connect = KafkaConnect(topic='output-topic-parking-violations-issued', group='test_group')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    html.Div([
        html.H4('TERRA Satellite Live Feed'),
        html.Div(id='live-update-text'),
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=1000, # in milliseconds
            n_intervals=0
        )
    ])
)

# Multiple components can update everytime interval gets fired.
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