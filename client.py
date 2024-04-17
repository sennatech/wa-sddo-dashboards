import asyncio
import socketio

# Cria um cliente Socket.IO
sio = socketio.AsyncClient()


@sio.event
async def connect():
    print("Conectado ao servidor Socket.IO")
    await sio.emit('response_data')  # Isso deve coincidir com o que o servidor espera

@sio.event
async def response_data(data):
    print("Dados recebidos do servidor:", data)

async def main():
    # Altere a URL para usar wss:// e assegure-se de que a porta está correta para WSS
    # Você também precisa garantir que o servidor suporta WSS nessa porta.
    await sio.connect('http://192.168.0.4:8054')  # Substitua com a URL correta para WSS
    await sio.wait()

if __name__ == '__main__':
    asyncio.run(main())