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
@app.server.route('/set_zipcode', methods=['POST'])
def set_holder_document_number():
    data = request.json
    zipcode = data.get('zip_code')
    causa = data.get('causa')

    with open('zip_code.txt', 'w') as file:
        file.write(zipcode if zipcode is not None else '')

    with open('causa.txt', 'w') as file:
        file.write(causa if causa is not None else '')

    return {'success': True, 'zip_code': zipcode or 'reset',
            'causa': causa or 'reset'}

#gerar arquivos
def get_zip_code_from_file():
    try:
        with open('zip_code.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

#gerar arquivos
def get_causa_from_file():
    try:
        with open('causa.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None


# Callback para o novo gráfico de colunas
@app.callback(
    Output('dashPeriodo', 'figure'),
    [Input('interval-component', 'n_intervals')]
)

def dash_coataçao_zipcode(n):
    zip_code = get_zip_code_from_file()
    causa = get_causa_from_file()
    (table_sinistro, table_emissoes, table_cotacoes, table_sinistros_unica, table_emissoes_unica, table_sinistro_tempo_medio, sinistros_aprovados, sinistros_recusados, sinistros_em_aberto,
    sinistros_fechados, tempo_medio_resposta, ticket_medio, ticket_medio_policy, preco_medio_cotação, apolices_ativas) = tb.retorna_dados()
    if zip_code == None or zip_code == "":
        df_filtrado_emissoes = table_emissoes_unica
        df_cotacao_filtrada = table_cotacoes
    else:
        df_cotacao_filtrada = table_cotacoes.filter(pl.col('zipcode') == zip_code)
        df_filtrado_emissoes = table_emissoes_unica.filter(pl.col('insured_address_zipcode') == zip_code)
    # print(df_cotacao_filtrada.head(5))
    fig_cotacao_emissao = fc.plotar_grafico_pizza(df_cotacao_filtrada,df_filtrado_emissoes,"Cotações","Emissões","Cotações X Emissões(Geral)")

    return fig_cotacao_emissao


@app.callback(
    Output('dashGenero', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def sinistros_por_causa(n):
    zip_code = get_zip_code_from_file()
    causa = get_causa_from_file()
    (table_sinistro, table_emissoes, table_cotacoes, table_sinistros_unica, table_emissoes_unica, table_sinistro_tempo_medio, sinistros_aprovados, sinistros_recusados, sinistros_em_aberto,
    sinistros_fechados, tempo_medio_resposta, ticket_medio, ticket_medio_policy, preco_medio_cotação, apolices_ativas) = tb.retorna_dados()
    if causa == None or causa == "":
        df_filtrado = table_sinistros_unica
    else:
        df_filtrado = table_sinistros_unica.filter(pl.col('notificationType') == causa)
    fig = fc.plotar_grafico_barras_pl(df_filtrado,"Sinistros: Avisados" + f"({str(causa)})")

    return fig



# Rodar o aplicativo
if __name__ == '__main__':
    #Com o debug False ele fica sem aquele botão azul
    app.run_server(debug=False, host='127.0.0.1', port=8054)