#esse funciona
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from db_config import connect_to_db, disconnect_from_db
from flask import request

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
@app.server.route('/set_holder_document_number', methods=['POST'])
def set_holder_document_number():
    data = request.json
    holder_doc_number = data.get('holder_document_number')
    policy_number = data.get('policy_number')

    with open('holder_doc_number.txt', 'w') as file:
        file.write(holder_doc_number if holder_doc_number is not None else '')

    with open('policy_number.txt', 'w') as file:
        file.write(policy_number if policy_number is not None else '')

    return {'success': True, 'holder_document_number': holder_doc_number or 'reset',
            'policy_number': policy_number or 'reset'}

#gerar arquivos
def get_policy_number_from_file():
    try:
        with open('policy_number.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

#gerar arquivos
def get_holder_doc_number_from_file():
    try:
        with open('holder_doc_number.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None


# Callback para o novo gráfico de colunas
@app.callback(
    Output('dashPeriodo', 'figure'),
    [Input('interval-component', 'n_intervals')]
)

def dashVendasPeriodo(n):
    holder_doc_number = get_holder_doc_number_from_file()
    policy_number = get_policy_number_from_file()
    conn = connect_to_db()  # Conectar ao banco de dados

    # Inicializar a consulta SQL base
    query = 'SELECT policy_validity_start, policy_amount FROM dbo.vw_emissoes_combinada WHERE 1=1'

    if holder_doc_number:
        # Adicionar filtro na consulta SQL para holder_document_number
        query += f" AND holder_document_number = '{holder_doc_number}'"

    if policy_number:
        # Adicionar filtro na consulta SQL para policy_number
        query += f" AND policy_number = '{policy_number}'"

    df = pd.read_sql_query(query, conn)  # Executar a consulta SQL construída
    disconnect_from_db(conn)  # Desconectar do banco de dados

    fig = px.bar(df, x='policy_validity_start', y='policy_amount', title='Vendido por período')
    fig.update_layout(
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        font_color='black'
    )
    return fig


@app.callback(
    Output('dashGenero', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def VendasPorGenero(n):
    holder_doc_number = get_holder_doc_number_from_file()
    policy_number = get_policy_number_from_file()
    conn = connect_to_db()  # Conectar ao banco de dados

    # Inicializar a consulta SQL base
    query = 'SELECT holder_gender, SUM(policy_amount) as total_policy_amount FROM sddo.emissoes WHERE 1=1'

    if holder_doc_number:
        # Adicionar filtro na consulta SQL para holder_document_number
        query += f" AND holder_document_number = '{holder_doc_number}'"

    if policy_number:
        # Adicionar filtro na consulta SQL para policy_number
        query += f" AND policy_number = '{policy_number}'"

    query += ' GROUP BY holder_gender'  # Adicionar a cláusula GROUP BY

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
    #Com o debug False ele fica sem aquele botão azul
    app.run_server(debug=False, host='127.0.0.1', port=8054)
