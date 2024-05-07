import socketio
from datetime import datetime

# Crie um cliente SocketIO
sio = socketio.Client()

# Define o evento de conexão
@sio.event
def connect():
    print("Connected to the server.")
    # Solicita os filtros iniciais ao conectar
    sio.emit('request_filters')

# Define o evento de recebimento de filtros
@sio.event
def receive_filters(data):
    print("Received filters from server:", data)

# Define o evento para a confirmação de atualização de filtros
@sio.event
def filters_updated(data):
    print("Filters update confirmation:", data)

# Define o evento de desconexão
@sio.event
def disconnect():
    print("Disconnected from the server.")

# Define um evento para receber dados
@sio.event
def response_data(data):
    print("Received data from server:", data)

# Função para enviar novos filtros
def send_filters():
    new_filters = {
        # 'document_number': ['1234567890'],
        'estado': ['SP'],
        # 'causa': ['Cobertura A', 'Cobertura B'],
        # 'data': [datetime.now().strftime('%Y-%m-%d')],  # Enviar a data atual como exemplo
        # 'sinistro': ['001', '002'],
        'reset': False
    }
    sio.emit('update_filters', new_filters)
    print("Sent new filters to server:", new_filters)

# Conectar ao servidor
try:
    sio.connect('http://localhost:8054')
    # Enviar novos filtros após a conexão
    send_filters()

    # Implementar um loop infinito para manter o cliente rodando
    print("Press Ctrl+C to disconnect...")
    while True:
        sio.sleep(1)  # Adicione um sleep para evitar uso intensivo da CPU

except KeyboardInterrupt:
    print("Disconnecting due to user interruption.")

except Exception as e:
    print("Failed to connect to the server:", e)

finally:
    # Desconectar do servidor
    sio.disconnect()
    print("Disconnected from the server.")
