import ssl
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import tables_e_queries as tb
import funcoes_e_driver as fc
import polars as pl
from datetime import date
from threading import Thread

# Configurar localização

# Instanciar o aplicativo Flask
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configurar cookies de sessão
app.config.update(
    SESSION_COOKIE_SAMESITE='None',
    SESSION_COOKIE_SECURE=True
)


# Inicializar filtros como dicionário no contexto da aplicação

def load_cpfs():
    connection = fc.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT document_number FROM sddo.cotacoes")
            cpfs = [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
    return cpfs


def load_estados():
    connection = fc.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT address_state FROM sddo.cotacoes")
            estados = [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
    return estados


def load_causas():
    connection = fc.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT coverage FROM sddo.cotacoes")
            causas = [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
    return causas


def load_initial_filters():
    cpfs = load_cpfs()
    estados = load_estados()
    causas = load_causas()

    # Atualizar os filtros no contexto da aplicação
    app.config["filters"] = {
        "cpfs": cpfs,
        "estados": estados,
        "causas": causas
    }


# Definir uma rota Flask para receber os filtros via POST
@app.route('/filtros', methods=['POST'])
def set_filtros():
    data = request.json

    # Verifica se a lista de CPFs é vazia e carrega todos os CPFs se for
    if data.get('document_number') == [""]:
        cpfs = load_cpfs()
    else:
        cpfs = data.get('document_number', load_cpfs())  # Usa o fallback para recarregar se não especificado

    # Verifica se a lista de estados é vazia e carrega todos os estados se for
    if data.get('estado') == [""]:
        estados = load_estados()
    else:
        estados = data.get('estado', load_estados())  # Usa o fallback para recarregar se não especificado

    # Verifica se a lista de causas é vazia e carrega todas as causas se for
    if data.get('causa') == [""]:
        causas = load_causas()
    else:
        causas = data.get('causa', load_causas())  # Usa o fallback para recarregar se não especificado

    # Atualizar os filtros no contexto da aplicação
    app.config["filters"] = {
        "cpfs": cpfs,
        "estados": estados,
        "causas": causas
    }

    return jsonify({'success': True, 'document_number': cpfs, 'estado': estados, 'causa': causas})


@app.route('/filtro_sinistros', methods=['GET'])
def get_clients():
    # Estabelecer conexão com o banco de dados
    connection = fc.get_connection()

    try:
        with connection.cursor() as cursor:
            # Consulta para obter todos os CPFs únicos
            cursor.execute("SELECT DISTINCT document_number FROM sddo.sinistros")
            cpfs = [row[0] for row in cursor.fetchall()]

            # Consulta para obter todos os estados únicos
            cursor.execute("SELECT DISTINCT state FROM sddo.sinistros")
            estados = [row[0] for row in cursor.fetchall()]

            # Consulta para obter todas as causas únicas (notificationType)
            cursor.execute("SELECT DISTINCT notificationType FROM sddo.sinistros")
            causas = [row[0] for row in cursor.fetchall()]

    finally:
        connection.close()

    return jsonify({"Cpfs": cpfs, "Estados": estados, "Causas": causas})


def background_task():
    """Função que roda em background para enviar dados periodicamente."""

    while True:
        (table_sinistro, table_emissoes, table_cotacoes,
         table_sinistro_tempo_medio, tempo_medio_resposta, ticket_medio, ticket_medio_policy, preco_medio_cotação,
         apolices_ativas) = tb.retorna_dados(cpfs=app.config["filters"]["cpfs"],
                                                  estados=app.config["filters"]["estados"],
                                                  causas=app.config["filters"]["causas"])

        df_cotacao_filtrada = table_cotacoes.unique(subset="id")

        df_filtrado_emissoes = table_emissoes.unique(subset="id")

        # df_filtrado_sinistros = table_sinistros_unica.filter(
        #     (pl.col('document_number').is_in(cpfs)) &
        #     (pl.col('state').is_in(estados)) &
        #     (pl.col('notificationType').is_in(causas)))
        df_filtrado_sinistros = table_sinistro.unique(subset="id")
        print(df_filtrado_sinistros.columns)

        dicionario_cotacao = fc.retorna_valores_quantidade_por_tempo(
            df_cotacao_filtrada)
        dicionario_emissao = fc.retorna_valores_quantidade_por_tempo(
            df_filtrado_emissoes)
        dicionario_sinistro = fc.retorna_valores_quantidade_por_tempo(
            df_filtrado_sinistros)


        apolices_em_desuso = fc.calcular_porcentagem_ids_unicos_pl(df_filtrado_sinistros, df_filtrado_emissoes,
                                                                   coluna_id="id")

        tipos_notificacao_e_porcentagens = fc.calcular_porcentagem_notificationType_e_retornar_lista(
            df_filtrado_sinistros)

        status_sinistro_M, status_sinistro_F = fc.retorna_valores_genero(df_filtrado_sinistros)

        num_cotacoes, num_emissoes = fc.retorna_valores_cotacao_emissao(df_cotacao_filtrada, df_filtrado_emissoes)

        (recusado, pendente, aprovado, porcentagem_recusado, porcentagem_pendente, porcentagem_aprovado,
         porcentagem_sinistro_aberto, porcentagem_sinistro_fechado,
         porcentagem_sinistros_pagos, porcentagem_sinistros_recusados) = fc.retorna_status_sinistro(
            df_filtrado_sinistros)

        sinistro_por_estado = fc.retorna_sinistro_por_estado(df_filtrado_sinistros)
        cotacoes = df_cotacao_filtrada.shape[0]
        contratacoes = df_filtrado_emissoes.shape[0]
        ticket_medio = round(df_filtrado_emissoes["amount"].mean())
        ticket_total = round(df_filtrado_emissoes["amount"].sum())
        apolices_ativas = df_filtrado_emissoes.shape[0]
        total_sinistros = df_filtrado_sinistros.shape[0]
        tempo_medio_resposta = tempo_medio_resposta




        arrays = {
            "sinistro_M": status_sinistro_M,
            "sinistro_F": status_sinistro_F,
            "cotacoes": num_cotacoes,
            "emissoes": num_emissoes,
            "recusado": recusado,
            "pendente": pendente,
            "aprovado": aprovado,
            "ticket_medio": ticket_medio,
            "ticket_total": ticket_total,
            "tempo_medio_resposta": tempo_medio_resposta,
            "total_sinistros": total_sinistros,
            "apolices_ativas": apolices_ativas,
            "sinistros_por_estado": sinistro_por_estado,  # Assume-se que este já esteja em formato de dicionário
            "valores_por_data_sinisto": dicionario_sinistro,
            "valores_por_data_cotacao": dicionario_cotacao,
            "valores_por_data_emissao": dicionario_emissao,
            "porcentagem_apolices_em_desuso": apolices_em_desuso,
            "porcentagem_recusado": porcentagem_recusado,
            "porcentagem_pendente": porcentagem_pendente,
            "porcentagem_aprovado": porcentagem_aprovado,
            "porcentagem_notificacoes_pendente": tipos_notificacao_e_porcentagens,
            "porcentagem_sinistros_aberto": porcentagem_sinistro_aberto,
            "porcentagem_sinistros_fechado": porcentagem_sinistro_fechado,
            "porcentagem_sinistros_pagos": porcentagem_sinistros_pagos,
            "porcentagem_sinistros_recusados": porcentagem_sinistros_recusados
        }

        # Converter os dados para JSON serializáveis
        data = fc.process_data(arrays)
        # Usar o emit dentro do contexto do SocketIO para enviar para todos os clientes conectados
        socketio.emit('response_data', data)
        socketio.sleep(1)

        # Aguardar 3 segundos antes de enviar o próximo conjunto de dados


@socketio.on('connect')
def on_connect():
    print('Client connected')
    # Iniciar a tarefa de background quando um cliente se conectar
    socketio.start_background_task(background_task)

def start_background_task():
    thread = Thread(target=background_task())
    thread.start()

def load_initial_filters_thread():
    thread = Thread(target=load_initial_filters)
    thread.start()


if __name__ == '__main__':
    # Create a thread to load filters
    filter_thread = Thread(target=load_initial_filters)

    # Start the thread
    filter_thread.start()

    # Wait for the filter loading thread to complete
    filter_thread.join()

    # Start the Flask-SocketIO server
    print("Starting server...")
    socketio.run(app, host='0.0.0.0', port=8054, debug=True, allow_unsafe_werkzeug=True)
