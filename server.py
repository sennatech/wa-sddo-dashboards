import ssl
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import tables_e_queries as tb
import funcoes_e_driver as fc
import locale
import polars as pl
# Configurar localização
locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

# Instanciar o aplicativo Flask
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configurar cookies de sessão
app.config.update(
    SESSION_COOKIE_SAMESITE='None',
    SESSION_COOKIE_SECURE=True
)

# Definir uma rota Flask para receber os filtros via POST
@app.route('/filtros', methods=['POST'])
def set_filtros():
    data = request.json
    cpf = data.get('document_number')
    estado = data.get('estado')
    causa = data.get('causa')

    with open('document_number.txt', 'w') as file:
        for item in cpf:
            file.write(str(item) + '\n')

    with open('causa.txt', 'w') as file:
        for item in causa:
            file.write(str(item) + '\n')

    with open('estado.txt', 'w') as file:
        for item in estado:
            file.write(str(item) + '\n')

    return jsonify({'success': True, 'document_number': cpf or 'reset',
                    'causa': causa or 'reset', "estado": estado or "reset"})

# Função para ler CPFs de um arquivo
def get_cpf_from_file():
    try:
        with open('document_number.txt', 'r') as file:
            cpfs = file.read().strip().split('\n')
            return cpfs
    except FileNotFoundError:
        return None

# Função para ler causas de um arquivo
def get_causa_from_file():
    try:
        with open('causa.txt', 'r') as file:
            causas = file.read().strip().split('\n')
            return causas
    except FileNotFoundError:
        return None

# Função para ler estados de um arquivo
def get_estado_from_file():
    try:
        with open('estado.txt', 'r') as file:
            estados = file.read().strip().split('\n')
            return estados
    except FileNotFoundError:
        return None



# Função para processar filtros e salvar em arquivos
def process_and_save_filters(data):
    cpf = data.get('document_number', [])
    estado = data.get('estado', [])
    causa = data.get('causa', [])

    # Salva CPFs
    with open('document_number.txt', 'w') as file:
        for item in cpf:
            file.write(str(item) + '\n')

    # Salva causas
    with open('causa.txt', 'w') as file:
        for item in causa:
            file.write(str(item) + '\n')

    # Salva estados
    with open('estado.txt', 'w') as file:
        for item in estado:
            file.write(str(item) + '\n')

    return {'success': True, 'document_number': cpf or 'reset', 'causa': causa or 'reset', "estado": estado or "reset"}


def background_task():
    """Função que roda em background para enviar dados periodicamente."""
    while True:

        cpfs = get_cpf_from_file()
        causas = get_causa_from_file()
        estados = get_estado_from_file()

        (table_sinistro, table_emissoes, table_cotacoes, table_sinistros_unica, table_emissoes_unica,
         table_sinistro_tempo_medio, sinistros_aprovados, sinistros_recusados, sinistros_em_aberto,
         sinistros_fechados, tempo_medio_resposta, ticket_medio, ticket_medio_policy, preco_medio_cotação,
         apolices_ativas,
         table_cotacoes_unica) = tb.retorna_dados()

        if cpfs is None or cpfs == [""]:
            cpfs = table_cotacoes["document_number"].unique()
            # print(cpfs)
            # print(type(cpfs))

        if estados is None or estados == [""]:
            estados = table_cotacoes["address_state"].unique()
            # print(estados)
            # print(type(estados))

        if causas is None or causas == [""]:
            causas = table_emissoes["coverage_name"].unique()
            # print(causas)
            # print(type(causas))

        df_cotacao_filtrada = table_cotacoes.filter(
            (pl.col('document_number').is_in(cpfs)) &
            (pl.col('address_state').is_in(estados)) &
            (pl.col('coverage').is_in(causas)))
        df_cotacao_filtrada = df_cotacao_filtrada.unique(subset="id")

        df_filtrado_emissoes = table_emissoes.filter(
            (pl.col('document_number').is_in(cpfs)) &
            (pl.col('holder_address_state').is_in(estados)) &
            (pl.col('coverage_name').is_in(causas)))
        df_filtrado_emissoes = df_filtrado_emissoes.unique(subset="id")

        df_filtrado_sinistros = table_sinistros_unica.filter(
            (pl.col('document_number').is_in(cpfs)) &
            (pl.col('state').is_in(estados)) &
            (pl.col('notificationType').is_in(causas)))

        date_sinistro, quantidade_sinistro,percentua_sinistro = fc.retorna_valores_quantidade_por_tempo_sinistro(df_filtrado_sinistros)
        data_cotacao,quantidade_cotacao,percentua_cotacao = fc.retorna_valores_quantidade_por_tempo_cotacao(df_cotacao_filtrada)
        date_sinistro = date_sinistro.dt.strftime('%Y-%m-%d').tolist()
        quantidade_sinistro = quantidade_sinistro.to_list()
        date_emissao,quantidade_emissao,percentua_emissao = fc.retorna_valores_quantidade_por_tempo_emissao(df_filtrado_emissoes)

        status_sinistro_M,status_sinistro_F = fc.retorna_valores_genero(df_filtrado_sinistros)

        num_cotacoes, num_emissoes = fc.retorna_valores_cotacao_emissao(df_cotacao_filtrada,df_filtrado_emissoes)

        recusado,pendente,aprovado = fc.retorna_status_sinistro(df_filtrado_sinistros)

        estado_sinistro,sinistro_por_estado = fc.retorna_sinistro_por_estado(df_filtrado_sinistros)

        cotacoes = df_cotacao_filtrada.shape[0]
        contratacoes = df_filtrado_emissoes.shape[0]
        ticket_medio = round(df_filtrado_emissoes["amount"].mean())
        ticket_total = round(df_filtrado_emissoes["amount"].sum())
        apolices_ativas = df_filtrado_emissoes.shape[0]
        total_sinistros = df_filtrado_sinistros.shape[0]
        tempo_medio_resposta = tempo_medio_resposta
        ticket_medio = locale.currency(ticket_medio, grouping=True)
        ticket_total = locale.currency(ticket_total, grouping=True)

        print(f"quantidade list {data_cotacao}")

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
            "estados_sinistro": estado_sinistro,
            "sinistros_por_estado": sinistro_por_estado,
            "data_sinisto": date_sinistro,
            "quantidade_sinistro": quantidade_sinistro,
            "percentual_sinisto":percentua_sinistro,
            "data_cotacao":data_cotacao,
            "quantidade_cotacao":quantidade_cotacao,
            "percentual_cotacao": percentua_cotacao,
            "data_emissao":date_emissao,
            "quantidade_emissao":quantidade_emissao,
            "percentual_emissao":percentua_emissao



        }
        # Usar o emit dentro do contexto do SocketIO para enviar para todos os clientes conectados
        socketio.emit('response_data', arrays)

        # Aguardar 3 segundos antes de enviar o próximo conjunto de dados
        socketio.sleep(3)


@socketio.on('connect')
def on_connect():
    print('Client connected')
    # Iniciar a tarefa de background quando um cliente se conectar
    socketio.start_background_task(background_task)


if __name__ == '__main__':
    print("testando docker")
    print("...")
    socketio.run(app, host='0.0.0.0', port=8054, debug=True, allow_unsafe_werkzeug=True)
