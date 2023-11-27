import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects
import plotly.graph_objects as go
from dash import Dash, html, dcc
import tables_e_driver_sql as tb
import dash_bootstrap_components as dbc
import json
brazil_states = json.load(open("venv/brazil_geo.json", "r"))
table_mapa_emissoes = tb.table_emissoes_unica[["insured_address_state","policy_amount","coverage_sum_insured","eventtime"]]
table_mapa_emissoes_bh = table_mapa_emissoes[table_mapa_emissoes["insured_address_state"] == "BH"]
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
data_minima = tb.table_cotacoes["eventtime"].min()
print(data_minima)
data_maxima = tb.table_emissoes_unica["eventtime"].max()
print(data_maxima)
num_cotation = tb.table_cotacoes["amount"].count()
print(num_cotation)
#==============================================================================================================================

app.layout = dbc.Container(
    dbc.Row([
        dbc.Col([
            html.Div([
                html.H3("Dashboard Geral SDDO",style={"color":"#30679A"}),
            ], style={}),
            html.P("Informe a data na qual deseja obter informações:", style={"margin-top": "40px"}),
            html.Div(id = "div-teste",children = [
                dcc.DatePickerRange(id="date-picker",min_date_allowed=data_minima,
                                    max_date_allowed=data_maxima,
                                    initial_visible_month=data_minima,
                                    display_format="DD MM YYYY",
                                    style={"border": "0px solid black"}),
                dcc.DatePickerRange(id="date-picker", min_date_allowed=data_minima,
                                    max_date_allowed=data_maxima,
                                    initial_visible_month=data_minima,
                                    display_format="DD MM YYYY",
                                    style={"border": "0px solid black"})

            ]),
            # html.Div(id = "div-teste",children = [
            #     dcc.DatePickerRange(id="date-picker",min_date_allowed=data_minima,
            #                         max_date_allowed=data_maxima,
            #                         initial_visible_month=data_minima,
            #                         display_format="DD MM YYYY",
            #                         style={"border": "0px solid black"})
            #
            # ])

        ]),
        # dbc.Col([
        #     dcc.Graph(id="cloroplhepth-map",figure=fig)
        # ])
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Span("Cotações X Contratações"),
                        html.H3(style={"color": "#001322"}, id="cotacoes"),
                        html.H3(style={"color": "#001322"}, id="contrataçoes"),
                    ])
                ], color="light", outline=True,style={"margin-top":"10px",
                                                      "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                      "color": "#FFFFFF"})

            ], md=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Span("Sinistros em aberto"),
                        html.H3(style={"color": "#001322"}, id="sinistros_em_aberto"),
                    ])
                ], color="light", outline=True,style={"margin-top":"10px",
                                                      "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                      "color": "#FFFFFF"})

            ], md=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Span("Tempo medio de resposta em horas"),
                        html.H3(style={"color": "#001322"}, id="tempo_medio_resposta"),
                    ])
                ], color="light", outline=True,style={"margin-top":"10px",
                                                      "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                      "color": "#FFFFFF"})

            ], md=4)

        ])
    ]),

)

if __name__ == "__main__":
    app.run_server(debug=True)


