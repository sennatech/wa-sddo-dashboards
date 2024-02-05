from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from flask import request
import funcoes_e_driver_sql as fc
import tables_e_queries as tb
import polars as pl
import pyarrow
# Instanciar o aplicativo Dash

app = Dash(__name__, routes_pathname_prefix='/', requests_pathname_prefix='/')

#ativado para se quebrar a aplicação não aparecer para o cliente
app.config.suppress_callback_exceptions = True

# Configurar cookies de sessão
app.server.config.update(
    SESSION_COOKIE_SAMESITE='None',
    SESSION_COOKIE_SECURE=True
)

# Definir o layout do aplicativo
app.layout = html.Div([

    dcc.Graph(id='dashPeriodo'),
    dcc.Graph(id='dashGenero'),
    dcc.Interval(
        id='interval-component',
        interval=2 * 1000,  # em milissegundos
        n_intervals=0
    )
])

# @app.server.route definindo uma rota Flask no dash
@app.server.route('/filtros', methods=['POST'])
def set_filtros():
    data = request.json
    cpf = data.get('document_number')
    estado = data.get('estado')
    causa = data.get('causa')

    with open('document_number.txt', 'w') as file:
        for item in cpf:
            file.write(str(item) + '\n')

    with open('causa.txt', 'w') as file:
        file.write(causa if causa is not None else '')

    with open('estado.txt', 'w') as file:
        file.write(causa if causa is not None else '')

    return {'success': True, 'document_number': cpf or 'reset',
            'causa': causa or 'reset', "estado": estado or "reset"}

#gerar arquivos
def get_cpf_from_file():
    try:
        with open('document_number.txt', 'r') as file:
            cpfs = file.read().strip().split('\n')
            return cpfs
    except FileNotFoundError:
        return None

#gerar arquivos
def get_causa_from_file():
    try:
        with open('causa.txt', 'r') as file:
            causas = file.read().strip().split('\n')
            return causas
    except FileNotFoundError:
        return None

def get_estado_from_file():
    try:
        with open('estado.txt', 'r') as file:
            estado = file.read().strip().split('\n')
            return estado
    except FileNotFoundError:
        return None

# Callback para o novo gráfico de colunas
@app.callback(
    Output('dashPeriodo', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def dash_cotaçao_zipcode(n):
    cpfs = get_cpf_from_file()
    causas = get_estado_from_file()
    estados = get_estado_from_file()
    (table_sinistro, table_emissoes, table_cotacoes, table_sinistros_unica, table_emissoes_unica, table_sinistro_tempo_medio, sinistros_aprovados, sinistros_recusados, sinistros_em_aberto,
    sinistros_fechados, tempo_medio_resposta, ticket_medio, ticket_medio_policy, preco_medio_cotação, apolices_ativas) = tb.retorna_dados()
    if cpfs is None or cpfs == [""]:
        cpfs = table_cotacoes["document_number"].unique()
        print(cpfs)
        print(type(cpfs))

    if estados is None or estados == [""]:
        estados = table_cotacoes["state"].unique()
        print(estados)
        print(type(estados))

    if causas is None or causas == [""]:
        causas = table_emissoes["coverage_name"].unique()
        print(causas)
        print(type(causas))

    print(type(cpfs))
    print(cpfs)
    df_cotacao_filtrada = table_cotacoes.filter((pl.col('document_number').is_in(cpfs)) &
                                                    (pl.col('estado').is_in(estados)))

    df_filtrado_emissoes = table_emissoes_unica.filter(
        (pl.col('holder_document_number').is_in(cpfs)) &
        (pl.col('holder_address_state').is_in(estados)) &
        (pl.col('coverage_name').is_in(causas)))

    df_filtrado_sinistros = table_emissoes_unica.filter(
        (pl.col('insuredDocument').is_in(cpfs)) &
        (pl.col('state').is_in(estados)) &
        (pl.col('notificationType').is_in(causas)))

    print(df_cotacao_filtrada.head(5))
    fig = fc.plotar_grafico_pizza(df_cotacao_filtrada,df_filtrado_emissoes,"Cotações","Emissões","Cotações X Emissões(Geral)")

    return fig


@app.callback(
    Output('dashGenero', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def sinistros_por_causa(n):
    cpfs = get_cpf_from_file()
    causas = get_causa_from_file()
    estados = get_estado_from_file()
    (table_sinistro, table_emissoes, table_cotacoes, table_sinistros_unica, table_emissoes_unica, table_sinistro_tempo_medio, sinistros_aprovados, sinistros_recusados, sinistros_em_aberto,
    sinistros_fechados, tempo_medio_resposta, ticket_medio, ticket_medio_policy, preco_medio_cotação, apolices_ativas) = tb.retorna_dados()

    if cpfs is None or cpfs == [""]:
        cpfs = table_cotacoes["document_number"].unique()
        print(cpfs)
        print(type(cpfs))

    if estados is None or estados == [""]:
        estados = table_cotacoes["state"].unique()
        print(estados)
        print(type(estados))

    if causas is None or causas == [""]:
        causas = table_emissoes["coverage_name"].unique()
        print(causas)
        print(type(causas))


    df_cotacao_filtrada = table_cotacoes.filter((pl.col('document_number').is_in(cpfs)) &
                                                    (pl.col('estado').is_in(estados)))

    df_filtrado_emissoes = table_emissoes_unica.filter(
        (pl.col('holder_document_number').is_in(cpfs)) &
        (pl.col('holder_address_state').is_in(estados)) &
        (pl.col('coverage_name').is_in(causas)))

    df_filtrado_sinistros = table_emissoes_unica.filter(
        (pl.col('insuredDocument').is_in(cpfs)) &
        (pl.col('state').is_in(estados)) &
        (pl.col('notificationType').is_in(causas)))


    fig = fc.plotar_grafico_barras_pl(df_filtrado_sinistros,"Sinistros: Avisados")

    return fig


#     if cpfs is None or cpfs == [""]:
#         cpfs = table_cotacoes["document_number"].unique()
#         print(cpfs)
#         print(type(cpfs))
#
#     if estados is None or estados == [""]:
#         estados = table_cotacoes["state"].unique()
#         print(estados)
#         print(type(estados))
#
#     if causas is None or causas == [""]:
#         causas = table_emissoes["coverage_name"].unique()
#         print(causas)
#         print(type(causas))
#
#     print(type(cpfs))
#     print(cpfs)
#     df_cotacao_filtrada = table_cotacoes.filter((pl.col('document_number').is_in(cpfs)) &
#                                                     (pl.col('estado').is_in(estados)))
#
#     df_filtrado_emissoes = table_emissoes_unica.filter(
#         (pl.col('holder_document_number').is_in(cpfs)) &
#         (pl.col('holder_address_state').is_in(estados)) &
#         (pl.col('coverage_name').is_in(causas)))
#
#     df_filtrado_sinistros = table_emissoes_unica.filter(
#         (pl.col('insuredDocument').is_in(cpfs)) &
#         (pl.col('state').is_in(estados)) &
#         (pl.col('notificationType').is_in(causas)))


# Rodar o aplicativo
if __name__ == '__main__':
    #Com o debug False ele fica sem aquele botão azul
    app.run_server(debug=False, host='127.0.0.1', port=8054)