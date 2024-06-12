import textwrap
import pyodbc
import polars as pl

from credentials import username, password

# Conectando com o banco de dados
driver = '{ODBC Driver 18 for SQL Server}'
server = 'dbs-sddo-dev.database.windows.net,1433'
database = 'db-sddo-dev'
connection_string = textwrap.dedent(f'''
    Driver={driver};
    Server={server};
    Database={database};
    Uid={username};
    Pwd={password};
    Encrypt=yes;
    TrustServerCertificate=no;
    Connection Timeout=30;
    MultipleActiveResultSets=True;
''')


def get_connection():
    return pyodbc.connect(connection_string.strip())


def transforma_query_em_sql(sql_query):
    try:
        with get_connection() as cnxn:
            crsr = cnxn.cursor()
            df = pl.read_database(query=sql_query, connection=crsr)
            crsr.close()
#################################
#            # Certifique-se de que 'date' está em formato datetime, se aplicável
#            if 'date' in df.columns:
#                df = df.with_columns(pl.col('date').str.strptime(pl.Date, '%Y-%m-%d'))

            # Verificação: Imprima os dados carregados
 #           print("Dados carregados:")
 #           print(df)
#################################
            if df.shape[0] == 0:
                raise ValueError("DataFrame vazio após a consulta SQL.")
            return df
    except Exception as e:
        return f"Erro: {str(e)}"


def retorna_valores_quantidade_por_tempo(dataframe):
    try:
        # Agrupar por data e calcular o número de eventos
        eventos_por_tempo = dataframe.groupby('date').agg(pl.count().alias('quantidade'))
        if eventos_por_tempo.shape[0] == 0:
            raise ValueError("DataFrame vazio após o agrupamento.")

        # Ordenar resultados por data
        eventos_por_tempo = eventos_por_tempo.sort('date')

        # Calcular a variação percentual
        quantidade_shifted = eventos_por_tempo['quantidade'].shift(-1).fill_null(0)
        variacao_percentual = ((quantidade_shifted / eventos_por_tempo['quantidade'] - 1) * 100).round()

        # Criar um novo DataFrame adicionando a variação percentual
        eventos_por_tempo = eventos_por_tempo.with_columns(
            variacao_percentual.alias('variacao_percentual')
        )

        # Converter o DataFrame para uma lista de dicionários
        eventos_lista = [
            {'date': row['date'].isoformat(), 'quantidade': row['quantidade'], 'variacao_percentual': row['variacao_percentual']}
            for row in eventos_por_tempo.to_dicts()
        ]

        # print(f"Eventos por tempo: {eventos_lista}")
        return eventos_lista
    except Exception as e:
        # print(f"Erro: {str(e)}")
        return []



def retorna_valores_genero(df_filtrado_sinistros):
    try:
        # print(df_filtrado_sinistros.head(5))
        df_sinistros_M = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'M')
        df_sinistros_F = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'F')
        # print(f"df M {df_filtrado_sinistros["genderNotifier"]}")
        num_sinistros_M = df_sinistros_M.shape[0]
        num_sinistros_F = df_sinistros_F.shape[0]
        # print(f"M {num_sinistros_M}")
        # print(f"F {num_sinistros_F}")

        if num_sinistros_M == 0 and num_sinistros_F == 0:
            raise ValueError("Não foram encontrados registros para os gêneros especificados.")
        return num_sinistros_M, num_sinistros_F
    except Exception as e:
        print(e)
        return 0,0


def retorna_status_sinistro(df_sinistro_filtrada):
    # Calcular a contagem de cada valor na coluna especificada
    status_counts = df_sinistro_filtrada.group_by("status_sinistro").agg(pl.count().alias('count'))
    # Converter Polars DataFrame para Pandas DataFrame para usar com Plotly

    total_sinistros = len(df_sinistro_filtrada)

    # Filtrar e extrair os valores diretamente
    recusado_row = status_counts.filter(pl.col('status_sinistro') == 'RECUSADO').select('count')
    pendente_row = status_counts.filter(pl.col('status_sinistro') == 'PENDENTE').select('count')
    aprovado_row = status_counts.filter(pl.col('status_sinistro') == 'APROVADO').select('count')

    # Verificar se os DataFrames filtrados estão vazios e extrair o valor ou atribuir 0
    recusado_val = recusado_row['count'][0] if not recusado_row.is_empty() else 0
    pendente_val = pendente_row['count'][0] if not pendente_row.is_empty() else 0
    aprovado_val = aprovado_row['count'][0] if not aprovado_row.is_empty() else 0

    try:
        porcentagem_recusado = round((recusado_val / total_sinistros) * 100, 0)
    except:
        porcentagem_recusado = 0

    try:
        porcentagem_pendente = round((pendente_val / total_sinistros) * 100, 0)
    except:
        porcentagem_pendente = 0
    try:
        porcentagem_aprovado = round((aprovado_val / total_sinistros) * 100, 0)
    except:
        porcentagem_aprovado = 0

    try:
        sinistros_fechados = recusado_val + aprovado_val
        sinistro_em_aberto = pendente_val
        total_sinistros = sinistro_em_aberto + sinistros_fechados
        portentagem_sinistros_aberto = (round((sinistro_em_aberto / total_sinistros) * 100, 0))
        portentagem_sinistros_fechado = (round((sinistros_fechados / total_sinistros) * 100, 0))
    except:
        portentagem_sinistros_aberto = 0
        portentagem_sinistros_fechado = 0

    try:
        sinistros_fechados_total = recusado_val + aprovado_val
        porcentagem_sinistros_pagos = (round((aprovado_val / sinistros_fechados_total) * 100, 0))
        porcentagem_sinistros_recusados = (round((recusado_val / sinistros_fechados_total) * 100, 0))
    except:
        porcentagem_sinistros_pagos = 0
        porcentagem_sinistros_recusados = 0

    return (recusado_val, pendente_val, aprovado_val, porcentagem_recusado,
            porcentagem_pendente, porcentagem_aprovado, portentagem_sinistros_aberto,
            portentagem_sinistros_fechado, porcentagem_sinistros_pagos, porcentagem_sinistros_recusados)



def calc_status_stats(recusado, pendente, aprovado, total):
    try:
        porcentagem_recusado = round((recusado / total) * 100, 0) if total else 0
        porcentagem_pendente = round((pendente / total) * 100, 0) if total else 0
        porcentagem_aprovado = round((aprovado / total) * 100, 0) if total else 0
        sinistros_fechados = recusado + aprovado
        sinistro_em_aberto = pendente
        porcentagem_sinistros_aberto = round((sinistro_em_aberto / total) * 100, 0) if total else 0
        porcentagem_sinistros_fechado = round((sinistros_fechados / total) * 100, 0) if total else 0
        return (recusado, pendente, aprovado, porcentagem_recusado, porcentagem_pendente, porcentagem_aprovado,
                porcentagem_sinistros_aberto, porcentagem_sinistros_fechado)
    except ZeroDivisionError:
        return (0, 0, 0, 0, 0, 0, 0, 0)  # Tratamento para divisão por zero se total == 0


def calcular_porcentagem_ids_unicos_pl(df1, df2, coluna_id='id'):
    coluna_id_sinistros = "policy_number"
    # Encontra os IDs únicos em cada DataFrame
    ids_unicos_df1 = df1.select(coluna_id_sinistros).unique()
    ids_unicos_df2 = df2.select(coluna_id).unique()

    # Calcula a diferença de conjuntos para encontrar IDs em df1 que não estão em df2
    len_df1 = len(ids_unicos_df1)
    len_df2 = len(ids_unicos_df2)
    diferenca = len_df2 - len_df1

    porcentagem = round(((diferenca / len_df2) * 100), 0)
    # print(porcentagem)

    # print(ids_diferentes)
    # Calcula a porcentagem de IDs únicos em df1 em relação ao total de IDs únicos em df2

    return porcentagem



def calcular_porcentagem_notificationType_e_retornar_lista(df, coluna_notificationType='notificationType'):
    # Filtra o DataFrame para linhas onde o status é 'PENDENTE'
    df_pendente = df.filter(pl.col('status_sinistro') == 'PENDENTE')

    # Conta a quantidade de cada tipo de notificação e calcula a porcentagem
    contagem = df_pendente.groupby(coluna_notificationType).agg(pl.count().alias('quantidade'))
    total_notificacoes = contagem['quantidade'].sum()

    # Calcula a porcentagem de cada tipo de notificação
    contagem = contagem.with_columns(
        (pl.col('quantidade') / total_notificacoes * 100).round().alias('porcentagem')
    )

    # Ordenar a contagem por tipo de notificação para garantir consistência no retorno
    contagem = contagem.sort(coluna_notificationType)

    # Prepara os resultados para serem retornados como uma lista de dicionários
    resultado_lista = [
        {'tipo': tipo, 'porcentagem': percent} for tipo, percent in zip(contagem[coluna_notificationType].to_list(), contagem['porcentagem'].to_list())
    ]

    return resultado_lista

def retorna_valores_cotacao_emissao(df_cotacoes, df_emissoes):
    num_cotacoes = df_cotacoes.shape[0]
    num_emissoes = df_emissoes.shape[0]

    return num_cotacoes, num_emissoes


def retorna_sinistro_por_estado(df_sinistro_filtrado):
    try:
        # Ordenando os dados por estado antes do agrupamento
        df_ordenado = df_sinistro_filtrado.sort("state")

        # Agrupando os dados por estado e contando as ocorrências
        agrupado_por_estado = df_ordenado.groupby("state").agg(pl.count().alias('quantidade'))

        # Convertendo o resultado para uma lista de dicionários
        list_of_dicts = [
            {'state': row['state'], 'quantidade': row['quantidade']}
            for row in agrupado_por_estado.to_dicts()
        ]

        # Ordenando explicitamente a lista de dicionários por estado (opcional, dependendo da necessidade)
        list_of_dicts.sort(key=lambda x: x['state'])

        # Verificação se a lista está vazia
        if not list_of_dicts:
            raise ValueError("Lista vazia após o agrupamento por estado.")

        return list_of_dicts
    except Exception as e:
        print(e)
        return 0

def process_data(data):
    # Converter os dados para JSON serializáveis
    for key, value in data.items():
        if isinstance(value, pl.Series):
            data[key] = value.to_list()  # Converte Polars Series para lista
        elif isinstance(value, pl.DataFrame):
            data[key] = value.to_dicts()
    return data
