import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects
import plotly.graph_objects as go
import funcoes as fc
import funcoes1 as fc1
from dash import Dash, html, dcc
import tables_e_driver_sql as tb
import dash_bootstrap_components as dbc
import json
from dash import Dash, dcc, html, Input, Output, callback

brazil_states = json.load(open("venv/brazil_geo.json", "r"))
table_mapa_emissoes = tb.table_emissoes_unica[["insured_address_state","policy_amount","coverage_sum_insured","eventtime"]]
table_mapa_emissoes_bh = table_mapa_emissoes[table_mapa_emissoes["insured_address_state"] == "BH"]
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.MORPH])

media_resp_sinistro = tb.media_resp_sinistro

#external_stylesheets=[dbc.themes.QUARTZ]
# app.css.append_css({'external_url': '/static/reset.css'})
select_columns_causa_sinistro = tb.select_columns_causa_sinistro
# print(type(select_columns_causa_sinistro))
select_columns_state = tb.state_unico
# print(type(select_columns_state))
data_minima = tb.table_cotacoes["eventtime"].min()
data_maxima = tb.table_emissoes_unica["eventtime"].max()
num_cotation = tb.table_cotacoes["amount"].count()


#==============================================================================================================================

app.layout = (
    dbc.Container(
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H3("Dashboard Geral SDDO",style={"color":"#30679A"}),
                ], style={}),
        dbc.Row([
            dbc.Col([
                html.P("Informe a data na qual deseja obter informações:", style={"margin-top": "50px","color":"#31999A","text-align": "center"}),
                html.Div(id="div-teste", children=[
                    dcc.DatePickerRange(id="date-picker", min_date_allowed=data_minima,
                                        max_date_allowed=data_maxima,
                                        initial_visible_month=data_minima,
                                        display_format="DD MM YYYY",
                                        style={"border": "0px solid black","text-align": "center"}),

                ])
            ],md=4,style={"text-align": "center"}),
    dbc.Col([
                html.P("Informe a causa do sinistro que deseja visualizar:", style={"margin-top": "50px","color":"#31999A","text-align": "center"}),
                dcc.Dropdown(id="causa-dropdown",options=select_columns_causa_sinistro
                             )
    ],md=4,style={"text-align": "center"}),
    dbc.Col([
                html.P("Informe o estado que deseja visualizar:", style={"margin-top": "50px","color":"#31999A","text-align": "center"}),
                dcc.Dropdown(id="state-dropdown",
                             options=select_columns_state
                             )
    ],md=4,style={"text-align": "center"}),
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
                    ], color="#30679A", outline=True,style={"margin-top":"10px",
                                                          "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                          "color": "#31999A","text-align": "center"})
#,"height":"10vh"
                ], md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Sinistros em aberto"),
                            html.H3(style={"color": "#001322","text-align": "center"}, id="sinistros_em_aberto"),
                        ])
                    ], color="#30679A", outline=True,style={"margin-top":"10px",
                                                          "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                          "color": "#31999A","text-align": "center"})

                ], md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Tempo medio de resposta"),
                            html.H3(style={"color": "#001322"}, id="tempo_medio_resposta"),
                            html.H6("(em horas)", style={"color": "#31999A","text-align": "center"}),

                        ])
                    ], color="#30679A", outline=True,style={"margin-top":"10px",
                                                          "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                          "color": "#31999A","text-align": "center"})

                ], md=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Apolices Ativas"),
                            html.H3(style={"color": "#001322","text-align": "center"}, id="Apolices-ativas"),
                        ])
                    ], color="#30679A", outline=True, style={"margin-top": "10px",
                                                           "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                           "color": "#31999A","text-align": "center"})

                ], md=3)

            ])
        ]),

    ))



@app.callback(
    [
        Output("cotacoes", "children"),
        Output("contrataçoes", "children"),
        Output("sinistros_em_aberto", "children"),
        Output("tempo_medio_resposta", "children"),
        Output("Apolices-ativas", "children"),
    ], [Input("date-picker", "date")]
)
def display_status(date, location):
    # print(location, date)
    # df_data_on_date =
    return None
    recuperados_novos = "-" if df_data_on_date["Recuperadosnovos"].isna().values[0] else f'{int(df_data_on_date["Recuperadosnovos"].values[0]):,}'.replace(",", ".")
    acompanhamentos_novos = "-" if df_data_on_date["emAcompanhamentoNovos"].isna().values[0]  else f'{int(df_data_on_date["emAcompanhamentoNovos"].values[0]):,}'.replace(",", ".")
    casos_acumulados = "-" if df_data_on_date["casosAcumulado"].isna().values[0]  else f'{int(df_data_on_date["casosAcumulado"].values[0]):,}'.replace(",", ".")
    casos_novos = "-" if df_data_on_date["casosNovos"].isna().values[0]  else f'{int(df_data_on_date["casosNovos"].values[0]):,}'.replace(",", ".")
    obitos_acumulado = "-" if df_data_on_date["obitosAcumulado"].isna().values[0]  else f'{int(df_data_on_date["obitosAcumulado"].values[0]):,}'.replace(",", ".")
    obitos_novos = "-" if df_data_on_date["obitosNovos"].isna().values[0]  else f'{int(df_data_on_date["obitosNovos"].values[0]):,}'.replace(",", ".")
    return None

if __name__ == "__main__":
    app.run_server(debug=True)


