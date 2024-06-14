import ssl
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import tables_e_queries as tb
import funcoes_e_driver as fc
import polars as pl
from datetime import date, datetime
from threading import Thread
from flask_socketio import disconnect

# Configurar localização

# Instanciar o aplicativo Flask
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configurar cookies de sessão
app.config.update(
    SESSION_COOKIE_SAMESITE='None',
    SESSION_COOKIE_SECURE=True
)

client_tasks = {}


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


def load_datas():
    connection = fc.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT date FROM sddo.cotacoes")
            datas = [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
    return datas


def load_causas():
    connection = fc.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT DISTINCT coverage FROM sddo.cotacoes")
            causas = [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
    return causas


def load_sinistros():
    connection = fc.get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT DISTINCT id FROM sddo.sinistros")
            sinistros = [row[0] for row in cursor.fetchall()]
    finally:
        connection.close()
    return sinistros


def load_initial_filters():
    cpfs = load_cpfs()
    estados = load_estados()
    causas = load_causas()
    datas = load_datas()
    sinistros = load_sinistros()
    reset = True

    # Atualizar os filtros no contexto da aplicação
    app.config["filters"] = {
        "cpfs": cpfs,
        "estados": estados,
        "causas": causas,
        "datas": datas,
        "sinistros": sinistros,
        "reset":reset
    }


# Definir uma rota Flask para receber os filtros via POST


@app.route('/filtros', methods=['POST'])
def set_filtros():
    try:
        data = request.json

        # Verifica se a lista de CPFs é vazia e carrega todos os CPFs se for
        if data.get("reset"):
            reset = True
        else:
            reset = False
        if not data.get('document_number'):
            cpfs = load_cpfs()
        else:
            cpfs = data.get('document_number', load_cpfs())  # Usa o fallback para recarregar se não especificado

        # Verifica se a lista de estados é vazia e carrega todos os estados se for
        if not data.get('estado') :
            estados = load_estados()
        else:
            estados = data.get('estado', load_estados())  # Usa o fallback para recarregar se não especificado

        # Verifica se a lista de causas é vazia e carrega todas as causas se for
        if not data.get('causa'):
            causas = load_causas()
        else:
            causas = data.get('causa', load_causas())  # Usa o fallback para recarregar se não especificado

        # Verifica se a lista de datas é vazia e carrega todas as datas se for
        if not data.get('data'):
            datas = load_datas()
        else:
            datas = data.get('data', load_datas())
            datas = [datetime.strptime(date, '%Y-%m-%d').date() for date in datas]

        if not data.get('sinistro'):
            sinistros = load_sinistros()
        else:
            sinistros = data.get('sinistro', load_sinistros())

        # Atualizar os filtros no contexto da aplicação
        app.config["filters"] = {
            "cpfs": cpfs,
            "estados": estados,
            "causas": causas,
            "datas": datas,
            "sinistros": sinistros,
            "reset":reset
        }

        return jsonify({'success': True, 'document_number': cpfs, 'estado': estados, 'causa': causas, "data": datas,
                        "sinistros": sinistros,"reset":reset})

    except Exception as e:
        # Loga o erro e retorna uma mensagem de erro genérica
        app.logger.error(f"An error occurred: {e}")
        return jsonify({'success': False, 'message': 'An error occurred while processing your request'}), 500


@socketio.on('disconnect')
def on_disconnect():
    client_id = request.sid
    print(f'Client {client_id} disconnected')
    if client_id in client_tasks:
        client_tasks[client_id] = False  # Sinalizando para a tarefa parar
        print(f'Task marked to stop for client {client_id}')


@app.route('/filtro_sinistros', methods=['GET'])
def get_filters():
    # Estabelecer conexão com o banco de dados
    connection = fc.get_connection()

    try:
        cpfs = load_cpfs()
        estados = load_estados()
        causas = load_causas()
        datas = load_datas()
        sinistros = load_sinistros()

    finally:
        connection.close()

    return jsonify({"Cpfs": cpfs, "Estados": estados, "Causas": causas, "Datas": datas, "Sinistros": sinistros})


def background_task(client_id):
    """Função que roda em background para enviar dados periodicamente."""
    try:
        while client_tasks.get(client_id, False):
            print(f'Preparing to send data to client {client_id}')

            (table_sinistro, table_emissoes, table_cotacoes,
             table_sinistro_tempo_medio, tempo_medio_resposta, preco_medio_cotação,
             apolices_ativas) = tb.retorna_dados()
            if app.config["filters"]["reset"]:
                df_filtrado_cotacoes = table_cotacoes.unique(subset="id")
                df_filtrado_emissoes = table_emissoes.unique(subset="id")
                df_filtrado_sinistros = table_sinistro
            else:
                df_filtrado_cotacoes = table_cotacoes.filter(
                    (pl.col('document_number').is_in(app.config["filters"]["cpfs"])) &
                    (pl.col('address_state').is_in(app.config["filters"]["estados"])) &
                    (pl.col('coverage').is_in(app.config["filters"]["causas"])) &
                    (pl.col('date').is_in(app.config["filters"]["datas"]))  # Adicionando filtro de datas
                ).unique(subset="id")

                # Filtrar emissões por CPFs, estados, causas, e datas
                df_filtrado_emissoes = table_emissoes.filter(
                    (pl.col('document_number').is_in(app.config["filters"]["cpfs"])) &
                    (pl.col('holder_address_state').is_in(app.config["filters"]["estados"])) &
                    (pl.col('coverage_name').is_in(app.config["filters"]["causas"])) &
                    (pl.col('date').is_in(app.config["filters"]["datas"]))  # Adicionando filtro de datas
                ).unique(subset="id")

                # Filtrar sinistros por CPFs, estados, causas, datas, e IDs de sinistros
                df_filtrado_sinistros = table_sinistro.filter(
                    (pl.col('document_number').is_in(app.config["filters"]["cpfs"])) &
                    (pl.col('state').is_in(app.config["filters"]["estados"])) &
                    (pl.col('notificationType').is_in(app.config["filters"]["causas"])) &
                    (pl.col('date').is_in(app.config["filters"]["datas"])) #& # Adicionando filtro de datas
                    #(pl.col('id').is_in(app.config["filters"]["sinistros"]))  # Adicionando filtro por IDs de sinistros específicos
                )


            #Filtrando por data aqui, para a query não ficar gigantesca
            df_filtrado_cotacoes = df_filtrado_cotacoes.filter(pl.col("date").is_in(app.config["filters"]["datas"]))
            df_filtrado_emissoes = df_filtrado_emissoes.filter(pl.col("date").is_in(app.config["filters"]["datas"]))
            df_filtrado_sinistros = df_filtrado_sinistros.filter(pl.col("date").is_in(app.config["filters"]["datas"]))
            # print(f" antes de filtrar {df_filtrado_sinistros["id"].sort()}")

            #Filtrando novamente por sinistro
            print(app.config["filters"]["sinistros"])
            # df_filtrado_sinistros = df_filtrado_sinistros.filter(pl.col("id").is_in(app.config["filters"]["sinistros"]))
            print(f" na main {df_filtrado_sinistros.head(5)}")
            dicionario_cotacao = fc.retorna_valores_quantidade_por_tempo(
                df_filtrado_cotacoes)
            dicionario_emissao = fc.retorna_valores_quantidade_por_tempo(
                df_filtrado_emissoes)
            dicionario_sinistro = fc.retorna_valores_quantidade_por_tempo(
                df_filtrado_sinistros)

            apolices_em_desuso = fc.calcular_porcentagem_ids_unicos_pl(df_filtrado_sinistros, df_filtrado_emissoes,
                                                                       coluna_id="id")

            tipos_notificacao_e_porcentagens = fc.calcular_porcentagem_notificationType_e_retornar_lista(
                df_filtrado_sinistros)

            status_sinistro_M, status_sinistro_F = fc.retorna_valores_genero(df_filtrado_sinistros)

            num_cotacoes, num_emissoes = fc.retorna_valores_cotacao_emissao(df_filtrado_cotacoes, df_filtrado_emissoes)

            (recusado, pendente, aprovado, porcentagem_recusado, porcentagem_pendente, porcentagem_aprovado,
             porcentagem_sinistro_aberto, porcentagem_sinistro_fechado,
             porcentagem_sinistros_pagos, porcentagem_sinistros_recusados) = fc.retorna_status_sinistro(
                df_filtrado_sinistros)

            sinistro_por_estado = fc.retorna_sinistro_por_estado(df_filtrado_sinistros)
            cotacoes = df_filtrado_cotacoes.shape[0]
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
            print(f"dados enviados {data}")
            socketio.emit('response_data', data)
            print(f"Enviando dados para o cliente {client_id}")
            if not client_tasks.get(client_id, True):
                # Verifica se a tarefa deve continuar
                print(f'Stopping send operation for client {client_id}')

                break
            socketio.sleep(1)
    finally:
        print(f'Background task ending for client {client_id}')
        client_tasks.pop(client_id, None)  # Remove a entrada para limpeza

    # Aguardar 3 segundos antes de enviar o próximo conjunto de dados


@app.route('/health')
def health_check():
    return 'OK', 200


@socketio.on('connect')
def on_connect():
    client_id = request.sid
    client_tasks[client_id] = True  # Inicializa a tarefa como ativa
    print(f'Client {client_id} connected, starting background task.')
    socketio.start_background_task(background_task, client_id)


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
