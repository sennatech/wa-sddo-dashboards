import dash
from dash import html, dcc, callback, Input, Output

dash.register_page(__name__, path='/')

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
                    html.Div('Cotações x Contratações', className='card-title'),
                    html.Div(
                        [
                            html.Div(id='cotacoes', className='card-number'),
                            html.Div(id='contratacoes', className='card-number'),
                        ],
                        className='card-content'
                    ),
                ],
                className='three columns card'
            ),
            html.Div(
                [
                    html.Div('Ticket Médio', className='card-title'),
                    html.Div(
                        [
                            html.Div(id='ticket-medio', className='card-number'),
                        ],
                        className='card-content'
                    ),
                ],
                className='three columns card'
            ),
            html.Div(
                [
                    html.Div('Ticket Total', className='card-title'),
                    html.Div(
                        [
                            html.Div(id='ticket-total', className='card-number'),
                        ],
                        className='card-content'
                    ),
                ],
                className='three columns card'
            ),
            html.Div(
                [
                    html.Div('Tempo Médio de Resposta Sinistro', className='card-title'),
                    html.Div(
                        [
                            html.Div(id='tempo-medio', className='card-number'),
                        ],
                        className='card-content'
                    ),
                ],
                className='three columns card'
            ),
        ],
        className='row'
    ),
    html.Div(
        [
            html.Div(
                dcc.Graph(id='dash_status_sinistro'),
                className='eight columns'
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Div('Apólices Ativas', className='card-title'),
                            html.Div(
                                [
                                    html.Div(id='apolices-ativas', className='card-number'),
                                ],
                                className='card-content'
                            ),
                        ],
                        className='card'
                    ),
                    html.Div(
                        [
                            html.Div('Total de Sinistros', className='card-title'),
                            html.Div(
                                [
                                    html.Div(id='total-sinistros', className='card-number'),
                                ],
                                className='card-content'
                            ),
                        ],
                        className='card'
                    ),
                ],
                className='four columns'
            ),
        ],
        className='row'
    ),
],
    className='container'
)


@callback(
    Output('geral-output', 'children'),
    Input('geral-input', 'value')
)
def update_city_selected(input_value):
    return f'You selected: {input_value}'