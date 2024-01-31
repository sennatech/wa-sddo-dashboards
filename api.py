#FILTRO NÃO FUNCIONA
#arquivo só para historico
# Importar as bibliotecas necessárias
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from db_config import connect_to_db, disconnect_from_db
from flask import request, session
from flask_session import Session
from flask import Flask

# Instanciar o aplicativo Dash
server = Flask(__name__)
app = Dash(__name__, routes_pathname_prefix='/', requests_pathname_prefix='/')

# Configurar o Flask-Session
app.server.config['SESSION_TYPE'] = 'filesystem'  # Pode ser 'filesystem', 'mongodb', etc.
app.server.config['SESSION_PERMANENT'] = False  # Define as sessões para não serem permanentes
Session(app.server)


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

@app.callback(
    Output('new-column-chart', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def dashVendasPeriodo(n):
    holder_doc_number = session.get('holder_doc_number', "")  # Obtenha o valor de holder_doc_number da variável de sessão
    conn = connect_to_db()  # Conectar ao banco de dados
    if holder_doc_number and holder_doc_number != "":
        query = f"SELECT policy_validity_start, policy_amount FROM dbo.vw_emissoes_combinada WHERE holder_document_number = '{holder_doc_number}'"
    else:
        query = 'SELECT policy_validity_start, policy_amount FROM dbo.vw_emissoes_combinada'
    df = pd.read_sql_query(query, conn)
    disconnect_from_db(conn)
    fig = px.bar(df, x='policy_validity_start', y='policy_amount', title='Vendido por período')
    fig.update_layout(
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        font_color='black'
    )
    return fig

@app.callback(
    Output('bar-chart', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def VendasPorGenero(n):
    holder_doc_number = session.get('holder_doc_number', "")  # Obtenha o valor de holder_doc_number da variável de sessão
    print(holder_doc_number)

    conn = connect_to_db()  # Conectar ao banco de dados
    if holder_doc_number and holder_doc_number != "":
        query = f"SELECT holder_gender, SUM(policy_amount) as total_policy_amount FROM sddo.emissoes WHERE holder_document_number = '{holder_doc_number}' GROUP BY holder_gender"
    else:
        query = 'SELECT holder_gender, SUM(policy_amount) as total_policy_amount FROM sddo.emissoes GROUP BY holder_gender'
    df = pd.read_sql_query(query, conn)
    disconnect_from_db(conn)
    fig = px.bar(df, x='holder_gender', y='total_policy_amount', title='Vendas por faixa etária/gênero')
    fig.update_layout(
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        font_color='black'
    )
    return fig

# Rota para receber solicitações do Postman
@app.server.route('/api', methods=['POST'])
def api_endpoint():
    try:
        data = request.json  # Acessar o corpo da solicitação como JSON
        holder_doc_number = data.get('holder_doc_number')  # Obter o valor de 'holder_doc_number' do JSON

        if holder_doc_number is not None:
            # Atualize a variável de sessão com o valor do Postman
            session['holder_doc_number'] = holder_doc_number
            return {"message": f"Received holder_doc_number: {holder_doc_number}"}
        else:
            return {"message": "No holder_doc_number found in the request body"}
    except Exception as e:
        return {"message": f"Error: {str(e)}"}
# Rodar o aplicativo
if __name__ == '__main__':
    app.run_server(debug=True, host='127.0.0.1', port=8053)
