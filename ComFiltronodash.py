# Importar as bibliotecas necessárias
#arquivo só para historico
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from db_config import connect_to_db, disconnect_from_db

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
    # Input para holder_document_number
    html.Div([
        dcc.Input(
            id='input-holder-doc-number',
            type='text',
            placeholder='Digite o Holder Document Number...'
        )
    ]),

    # Os gráficos
    dcc.Graph(id='bar-chart'),
    dcc.Graph(id='new-column-chart'),
    dcc.Interval(
        id='interval-component',
        interval=5 * 1000,  # em milissegundos
        n_intervals=0
    )
])


# Callback para o novo gráfico de colunas
@app.callback(
    Output('new-column-chart', 'figure'),
    [Input('interval-component', 'n_intervals'), Input('input-holder-doc-number', 'value')]
)
def dashVendasPeriodo(n, holder_doc_number):
    conn = connect_to_db()  # Conectar ao banco de dados
    if holder_doc_number:
        # Adicionar filtro na consulta SQL
        query = f"SELECT policy_validity_start, policy_amount FROM dbo.vw_emissoes_combinada WHERE holder_document_number = '{holder_doc_number}'"
    else:
        query = 'SELECT policy_validity_start, policy_amount FROM dbo.vw_emissoes_combinada'
    df = pd.read_sql_query(query, conn)  # Usando Pandas para ler os dados do banco
    disconnect_from_db(conn)  # Desconectar do banco de dados
    fig = px.bar(df, x='policy_validity_start', y='policy_amount',
                 title='Vendido por período')  # Usar DataFrame do Pandas diretamente
    fig.update_layout(
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        font_color='black'
    )
    return fig


@app.callback(
    Output('bar-chart', 'figure'),
    [Input('interval-component', 'n_intervals'), Input('input-holder-doc-number', 'value')]
)
def VendasPorGenero(n, holder_doc_number):
    conn = connect_to_db()  # Conectar ao banco de dados
    if holder_doc_number:
        # Adicionar filtro na consulta SQL
        query = f"SELECT holder_gender, SUM(policy_amount) as total_policy_amount FROM sddo.emissoes WHERE holder_document_number = '{holder_doc_number}' GROUP BY holder_gender"
    else:
        query = 'SELECT holder_gender, SUM(policy_amount) as total_policy_amount FROM sddo.emissoes GROUP BY holder_gender'
    df = pd.read_sql_query(query, conn)
    disconnect_from_db(conn)  # Desconectar do banco de dados
    fig = px.bar(df, x='holder_gender', y='total_policy_amount', title='Vendas por faixa etária/gênero')
    fig.update_layout(
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        font_color='black'
    )
    return fig


# Rodar o aplicativo
if __name__ == '__main__':
    app.run_server(debug=False, host='127.0.0.1', port=8050)
