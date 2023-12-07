import dash
import queries
import pandas as pd
import plotly.express as px
import plotly.graph_objects
import plotly.graph_objects as go
import funcoes as fc
from dash import Dash, html, dcc
import tables_e_driver_sql as tb
import dash_bootstrap_components as dbc
import json
import time
from dash import Dash, dcc, html, Input, Output, callback


# brazil_states = json.load(open("venv/brazil_geo.json", "r"))
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
            dcc.Interval(
                id='interval-component',
                interval=2 * 1000,  # em milissegundos, atualização a cada 2 segundos
                n_intervals=0
            ),
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

                ]),
#                 html.Div([
#                     dcc.Graph(id='graph-with-slider'),
#                     dcc.Slider(
#                     data_minima,
#                     data_maxima,
#                     step=None,
#                     value=data_minima,
#                     marks={str(year): str(year) for year in df['year'].unique()},
#                     id='year-slider'
#     )
# ])
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


            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Sinistros Aprovados"),
                            html.H3(style={"color": "#001322","text-align": "center"}, id="sinistros_aprovados"),
                        ])
                    ], color="#30679A", outline=True,style={"margin-top":"20px",
                                                          "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                          "color": "#31999A","text-align": "center"})

                ], md=4),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Sinistros Recusados"),
                            html.H3(style={"color": "#001322"}, id="sinistros_Recusados"),
                        ])
                    ], color="#30679A", outline=True,style={"margin-top":"20px",
                                                          "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                          "color": "#31999A","text-align": "center"})

                ], md=4),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Ticket Medio/Prêmio total"),
                            html.H3(style={"color": "#001322","text-align": "center"}, id="ticket_medio"),
                        ])
                    ], color="#30679A", outline=True, style={"margin-top": "20px",
                                                           "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                           "color": "#31999A","text-align": "center"})

                ], md=4)


            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id="sinistros_avisados")
                , md=4),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Sinistros em Regulação"),
                            html.H3(style={"color": "#001322"}, id="sinistros_em_regulacao"),

                        ])
                    ], color="#30679A", outline=True,style={"margin-top":"20px",
                                                          "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                          "color": "#31999A","text-align": "center"})

                ], md=4),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.Span("Apolices X Sinistros"),
                            html.H3(style={"color": "#001322", "text-align": "center"}, id="Apolices_ativas2"),
                            html.H3(style={"color": "#001322","text-align": "center"}, id="apolices_x_sinistros"),
                        ])
                    ], color="#30679A", outline=True, style={"margin-top": "20px",
                                                           "box-shadow": "0 4px 4px 0 rgba(0,0,0,0.15), 0 4px 20px 0 rgba(0,0,0, 0.19),",
                                                           "color": "#31999A","text-align": "center"})

                ], md=4)


            ])
        ]),

    ))

def plotar_grafico_barras(dataframe):
    dataframe['date'] = pd.to_datetime(dataframe['date'])

    eventos_por_tempo = dataframe.groupby('date').size().reset_index(name='quantidade')
    fig = px.bar(eventos_por_tempo, x='date', y='quantidade', title='Quantidade de Eventos por Tempo')

    # Exiba o gráfico
    return fig
@app.callback(

            Output("cotacoes", "children"),
            Output("contrataçoes", "children"),
            Output("sinistros_em_aberto", "children"),
            Output("tempo_medio_resposta", "children"),
            Output("Apolices-ativas", "children"),
            Output("sinistros_avisados","figure"),

    [Input('interval-component', 'n_intervals')]
)
def update_graph(n_intervals):
    table_cotacoes = fc.transforma_query_em_sql(queries.select_sql_cotacoes)
    num_cotacoes  = table_cotacoes.shape[0]
    table_emissoes_unica = pd.DataFrame(fc.retira_duplicadas_calcula_validade_policy_emissao(fc.transforma_query_em_sql(queries.select_sql_emissoes))).reset_index()
    num_emissoes = table_emissoes_unica.shape[0]
    table_sinistros_unica = table_sinistros_unica = pd.DataFrame(fc.retira_duplicadas_sinistro(fc.transforma_query_em_sql(queries.select_sql_sinistros))).reset_index()
    sinistros_em_aberto = table_sinistros_unica[table_sinistros_unica["status_sinistro"] == "PENDENTE"].shape[0]
    media_resp_sinistro = fc.calcula_tempo_medio_aprovacao_sinistro(table_sinistros_unica)
    apolices_ativas = table_emissoes_unica.shape[0]
    sinistros_avisados = plotar_grafico_barras(table_sinistros_unica)



    return num_cotacoes,num_emissoes,sinistros_em_aberto,media_resp_sinistro,apolices_ativas,sinistros_avisados


# @app.callback(
#     [
#         Output("cotacoes", "children"),
#         Output("contrataçoes", "children"),
#         Output("sinistros_em_aberto", "children"),
#         Output("tempo_medio_resposta", "children"),
#         Output("Apolices-ativas", "children"),
#     ], [Input("date-picker", "date")]
# )
# def display_status(date):
#     num_cotation_1 = num_cotation
#     return (num_cotation_1,2,3,4,5)

# def display_status():
#     # print(location, date)
#     # df_data_on_date =
#     # return 1,2,3,4,5
#
#
#     recuperados_novos = "-" if df_data_on_date["Recuperadosnovos"].isna().values[0] else f'{int(df_data_on_date["Recuperadosnovos"].values[0]):,}'.replace(",", ".")
#     acompanhamentos_novos = "-" if df_data_on_date["emAcompanhamentoNovos"].isna().values[0]  else f'{int(df_data_on_date["emAcompanhamentoNovos"].values[0]):,}'.replace(",", ".")
#     casos_acumulados = "-" if df_data_on_date["casosAcumulado"].isna().values[0]  else f'{int(df_data_on_date["casosAcumulado"].values[0]):,}'.replace(",", ".")
#     casos_novos = "-" if df_data_on_date["casosNovos"].isna().values[0]  else f'{int(df_data_on_date["casosNovos"].values[0]):,}'.replace(",", ".")
#     obitos_acumulado = "-" if df_data_on_date["obitosAcumulado"].isna().values[0]  else f'{int(df_data_on_date["obitosAcumulado"].values[0]):,}'.replace(",", ".")
#     obitos_novos = "-" if df_data_on_date["obitosNovos"].isna().values[0]  else f'{int(df_data_on_date["obitosNovos"].values[0]):,}'.replace(",", ".")
#     return 1,2,3,4,5


if __name__ == "__main__":
    num_cotation = tb.table_cotacoes["amount"].count()
    app.run_server(debug=True)


