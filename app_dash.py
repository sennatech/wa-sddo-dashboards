# app_dash.py
#arquivo só para historico
#dash-simples
##DO INICIO
# Importar as bibliotecas necessárias
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# Criar a conexão com o banco de dados
conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'
                      'SERVER=dbs-sddo-dev.database.windows.net,1433;'
                      'DATABASE=db-sddo-dev;'
                      'UID=sennatechadmin;'
                      'PWD=adm@Sennatech;'
                      )

# Instanciar o aplicativo Dash
app = Dash(__name__, routes_pathname_prefix='/', requests_pathname_prefix='/')

app.config.suppress_callback_exceptions = True

# Configurar cookies de sessão
app.server.config.update(
    SESSION_COOKIE_SAMESITE='None',
    SESSION_COOKIE_SECURE=True
)

# Definir o layout do aplicativo
app.layout = html.Div([
    dcc.Graph(id='bar-chart'),
    # dcc.Graph(id='column-chart'),
    dcc.Graph(id='new-column-chart'),
    dcc.Interval(
        id='interval-component',
        interval=10*1000,
        n_intervals=0
    )
])

# Callback para atualizar o gráfico
@app.callback(
    Output('bar-chart', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def vendas_por_genero(n):
    df = pd.read_sql_query('SELECT holder_gender, SUM(policy_amount) as total_policy_amount FROM sddo.emissoes GROUP BY holder_gender', conn)
    fig = px.bar(df, x='holder_gender', y='total_policy_amount', title='Vendas por faixa etária/gênero')
    fig.update_layout(
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        font_color='black'
    )
    return fig


# @app.callback(
#     Output('column-chart', 'figure'),
#     [Input('interval-component', 'n_intervals')]
# )
# def ranking_de_coberturas(n):
#     df = pd.read_sql_query('SELECT coverage_name, COUNT(*) as Contagem FROM Tabela Agrupada GROUP BY coverage_name', conn)
#     fig = px.bar(df, x='Contagem', y='coverage_name', orientation='h', title='Ranking de Coberturas')
#     fig.update_layout(
#         plot_bgcolor='rgba(0, 0, 0, 0)',
#         paper_bgcolor='rgba(0, 0, 0, 0)',
#         font_color='black'
#     )
#     return fig


# Callback para o novo gráfico de colunas
@app.callback(
    Output('new-column-chart', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def novo_grafico_de_colunas(n):
    df = pd.read_sql_query('SELECT policy_validity_start, policy_amount FROM dbo.vw_emissoes_combinada', conn)
    fig = px.bar(df, x='policy_validity_start', y='policy_amount', title='Vendido por período')
    fig.update_layout(
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        font_color='black'
    )
    return fig


# Rodar o aplicativo
if __name__ == '__main__':
    app.run_server(debug=False)

