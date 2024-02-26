import asyncio
import socketio

# Cria um cliente Socket.IO
sio = socketio.AsyncClient()

@sio.event
async def connect():
    print("Conectado ao servidor Socket.IO")
    await sio.emit('request_data')  # Isso deve coincidir com o que o servidor espera

@sio.event
async def response_data(data):
    print("Dados recebidos do servidor:", data)

async def main():
    await sio.connect('http://localhost:5000')  # Assegure-se de que a URL está correta
    await sio.wait()

if __name__ == '__main__':
    asyncio.run(main())
