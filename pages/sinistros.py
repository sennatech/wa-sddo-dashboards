import dash
from dash import html, dcc, callback, Input, Output

dash.register_page(__name__)

layout = html.Div([
    dcc.Interval(
        id='interval-component',
        interval=2 * 1000,  # em milissegundos
        n_intervals=0
    ),
    html.Div(
        [
    html.Div(
        [
            html.Div(
                dcc.Graph(id='dash_sinistros_avisados'),
                className='eight columns'
            ),
        ],
        className='row'
    ),
],
    className='container'
)
    ]
)
