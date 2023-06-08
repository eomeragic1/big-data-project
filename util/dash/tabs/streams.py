from dash import dcc, html


def content_streams():
    return html.Div(children=[dcc.Graph(id='live-update-graph'),
                              dcc.Interval(
                                  id='interval-component',
                                  interval=1000,  # in milliseconds
                                  n_intervals=0
                              )
                              ])
