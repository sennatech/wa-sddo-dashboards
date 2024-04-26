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
            if df.shape[0] == 0:
                raise ValueError("DataFrame vazio após a consulta SQL.")
            return df
    except Exception as e:
        return f"Erro: {str(e)}"


def retorna_valores_quantidade_por_tempo_sinistro(dataframe):
    try:
        eventos_por_tempo = dataframe.groupby('date').agg(pl.count().alias('quantidade'))
        if eventos_por_tempo.shape[0] == 0:
            raise ValueError("DataFrame vazio após o agrupamento.")

        eventos_por_tempo = eventos_por_tempo.sort('date')

        quantidade_shifted = eventos_por_tempo['quantidade'].shift(-1).fill_null(0)
        variação_percentual = (quantidade_shifted / eventos_por_tempo['quantidade'] - 1) * 100

        eventos_por_tempo = eventos_por_tempo.with_columns(
            variação_percentual.round().alias('variação_percentual')
        )

        return eventos_por_tempo
    except Exception as e:
        return f"Erro: {str(e)}"


def retorna_valores_genero(df_filtrado_sinistros):
    try:
        df_sinistros_M = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'M')
        df_sinistros_F = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'F')
        num_sinistros_M = df_sinistros_M.shape[0]
        num_sinistros_F = df_sinistros_F.shape[0]
        if num_sinistros_M == 0 and num_sinistros_F == 0:
            raise ValueError("Não foram encontrados registros para os gêneros especificados.")
        return num_sinistros_M, num_sinistros_F
    except Exception as e:
        return f"Erro: {str(e)}"


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
        return ("N/A", "N/A", "N/A", 0, 0, 0, 0, 0)  # Tratamento para divisão por zero se total == 0


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
    print(porcentagem)

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

    # Prepara os resultados para serem retornados como uma lista de tuplas
    resultado_lista = [
        (tipo, percent) for tipo, percent in zip(contagem[coluna_notificationType].to_list(), contagem['porcentagem'].to_list())
    ]

    return resultado_lista

def retorna_valores_cotacao_emissao(df_cotacoes, df_emissoes):
    num_cotacoes = df_cotacoes.shape[0]
    num_emissoes = df_emissoes.shape[0]

    return num_cotacoes, num_emissoes

def retorna_valores_quantidade_por_tempo_sinistro_cotacao(dataframe, coluna_data='date'):
    # Assegura que a coluna de data está no formato de data corretamente, se não estiver, converte
    if dataframe[coluna_data].dtype != pl.Date:
        dataframe = dataframe.with_columns(dataframe[coluna_data].str.strptime(pl.Date, "%Y-%m-%d"))

        # Agrupa por data, somando as quantidades de eventos por dia
    eventos_agrupados = dataframe.groupby(coluna_data).agg(pl.count().alias('quantidade'))

    # Calculando a variação percentual de quantidade dia a dia
    variação_percentual = eventos_agrupados['quantidade'].pct_change() * 100

    eventos_agrupados = eventos_agrupados.with_columns(
        variação_percentual.round().alias('variação_percentual')
    ).fill_null(0)  # Preenche valores nulos, especialmente para a primeira entrada

    # Preparar os dados para retorno
    date_list = eventos_agrupados[coluna_data].to_list()
    quantidade_list = eventos_agrupados['quantidade'].to_list()
    variacao_percentual_list = eventos_agrupados['variação_percentual'].to_list()

    return date_list, quantidade_list, variacao_percentual_list


def retorna_valores_quantidade_por_tempo_cotacao(dataframe_cotacao):
    return retorna_valores_quantidade_por_tempo_sinistro_cotacao(dataframe_cotacao, 'date')

def retorna_valores_quantidade_por_tempo_emissao(dataframe_emissao):
    return retorna_valores_quantidade_por_tempo_sinistro_cotacao(dataframe_emissao, 'date')
def retorna_sinistro_por_estado(df_sinistro_filtrado):
    try:
        agrupado_por_estado = df_sinistro_filtrado.groupby("state").agg(pl.count().alias('count'))
        agrupado_por_estado = agrupado_por_estado.sort('count')
        if agrupado_por_estado.shape[0] == 0:
            raise ValueError("DataFrame vazio após o agrupamento por estado.")
        return agrupado_por_estado
    except Exception as e:
        return f"Erro: {str(e)}"
