# Base image
FROM python:3.12-slim

# Definir variáveis de ambiente para evitar interações durante a construção
ENV DEBIAN_FRONTEND=noninteractive

# Atualizar lista de pacotes e instalar utilitários necessários
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configurar o repositório da Microsoft e instalar pacotes específicos
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev g++

# Diretório de trabalho
WORKDIR /app

# Instalar pacotes Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Clonar repositório específico do Git
RUN git clone -b melhorias_no_codigo https://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@github.com/sennatech/poc-dashboards-python.git /app/poc-dashboards-python

# Copiar o resto da aplicação
COPY . .

# Expor a porta da aplicação
EXPOSE 8054

# Comando para rodar a aplicação
CMD ["/bin/sh", "-c", "cd /app/poc-dashboards-python && git pull origin melhorias_no_codigo  && python server.py"]

# Definir autor da imagem
LABEL authors=luyza