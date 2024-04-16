import textwrap
import pyodbc
import polars as pl
from credentials import username, password
import pandas as pd



#Conectando com o banco de dados
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


def transforma_query_em_sql(sql_query):
    cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
    crsr: pyodbc.Cursor = cnxn.cursor()
    # crsr.execute(sql_query)
    # colunas = [i[0] for i in crsr.description]
    # dados = crsr.fetchall()
    # data = [tuple(rows) for rows in dados]
    # df = pl.DataFrame(data=data)
    # df.columns = colunas
    df = pl.read_database(query=sql_query, connection=crsr)
    crsr.close()
    cnxn.close()
    return df

def retorna_valores_quantidade_por_tempo_sinistro(dataframe):
    eventos_por_tempo = dataframe.group_by('date').count().rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    eventos_por_tempo_pandas['variação_percentual'] = eventos_por_tempo_pandas['quantidade'].pct_change().mul(100).round(0)
    eventos_por_tempo_pandas['variação_percentual'] = eventos_por_tempo_pandas['quantidade'].pct_change().mul(100).round(0).fillna(0)

    variacao_percentual_list = eventos_por_tempo_pandas['variação_percentual'].tolist()
    # print(variacao_percentual_list)
    return  eventos_por_tempo_pandas["date"],eventos_por_tempo_pandas["quantidade"],variacao_percentual_list

def retorna_valores_genero(df_filtrado_sinistros):
    # Contagem de sinistros por gênero usando Polars

    df_sinistros_M = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'M')
    df_sinistros_F = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'F')
    num_sinistros_M = df_sinistros_M.shape[0]
    num_sinistros_F = df_sinistros_F.shape[0]

    return num_sinistros_M, num_sinistros_F

def retorna_valores_cotacao_emissao(df_cotacoes, df_emissoes):

    num_cotacoes = df_cotacoes.shape[0]
    num_emissoes = df_emissoes.shape[0]

    return num_cotacoes, num_emissoes
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
        porcentagem_recusado = round((recusado_val/total_sinistros)*100,0)
    except:
        porcentagem_recusado = 0

    try:
        porcentagem_pendente = round((pendente_val/total_sinistros)*100,0)
    except:
        porcentagem_pendente = 0
    try:
        porcentagem_aprovado = round((aprovado_val/total_sinistros)*100,0)
    except:
        porcentagem_aprovado = 0

    try:
        sinistros_fechados = recusado_val+aprovado_val
        sinistro_em_aberto = pendente_val
        total_sinistros = sinistro_em_aberto + sinistros_fechados
        portentagem_sinistros_aberto = (round((sinistro_em_aberto / total_sinistros)*100,0))
        portentagem_sinistros_fechado = (round((sinistros_fechados / total_sinistros)*100,0))
    except:
        portentagem_sinistros_aberto = 0
        portentagem_sinistros_fechado = 0

    try:
        sinistros_fechados_total = recusado_val + aprovado_val
        porcentagem_sinistros_pagos = (round((aprovado_val / sinistros_fechados_total)*100,0))
        porcentagem_sinistros_recusados = (round((recusado_val / sinistros_fechados_total)*100,0))
    except:
        porcentagem_sinistros_pagos = 0
        porcentagem_sinistros_recusados = 0

    return (recusado_val,pendente_val,aprovado_val,porcentagem_recusado,
            porcentagem_pendente,porcentagem_aprovado,portentagem_sinistros_aberto,
            portentagem_sinistros_fechado,porcentagem_sinistros_pagos,porcentagem_sinistros_recusados)

def retorna_sinistro_por_estado(df_sinistro_filtrado):

    agrupado_por_estado = df_sinistro_filtrado.group_by("state").agg(pl.count().alias('count'))
    agrupado_por_estado_pandas = agrupado_por_estado.to_pandas()
    agrupado_por_estado_pandas.sort_values(by='count', inplace=True)
    state_list = agrupado_por_estado_pandas['state'].tolist()
    count_list = agrupado_por_estado_pandas['count'].tolist()

    return state_list,count_list


def retorna_valores_quantidade_por_tempo_cotacao(dataframe_cotacao):
    eventos_por_tempo = dataframe_cotacao.groupby('eventtime').count().rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    eventos_por_tempo_pandas['eventtime'] = pd.to_datetime(eventos_por_tempo_pandas['eventtime'])
    eventos_por_tempo_pandas['date'] = eventos_por_tempo_pandas['eventtime'].dt.date
    grouped_df = eventos_por_tempo_pandas.groupby('date')['quantidade'].sum().reset_index()
    grouped_df['variação_percentual'] = grouped_df['quantidade'].pct_change().mul(100).round(0)
    grouped_df['variação_percentual'] = grouped_df['quantidade'].pct_change().mul(100).round(0).fillna(0)

    variacao_percentual_list = grouped_df['variação_percentual'].tolist()
    date_list = grouped_df['date'].astype(str).tolist()
    quantidade_list = grouped_df['quantidade'].tolist()

    return date_list,quantidade_list,variacao_percentual_list

def retorna_valores_quantidade_por_tempo_emissao(dataframe_emissao):
    eventos_por_tempo = dataframe_emissao.group_by('eventtime').count().rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    eventos_por_tempo_pandas['eventtime'] = pd.to_datetime(eventos_por_tempo_pandas['eventtime'])
    eventos_por_tempo_pandas['date'] = eventos_por_tempo_pandas['eventtime'].dt.date
    grouped_df = eventos_por_tempo_pandas.groupby('date')['quantidade'].sum().reset_index()
    grouped_df['variação_percentual'] = grouped_df['quantidade'].pct_change().mul(100).round(0)
    grouped_df['variação_percentual'] = grouped_df['quantidade'].pct_change().mul(100).round(0).fillna(0)
    variacao_percentual_list = grouped_df['variação_percentual'].tolist()
    print(variacao_percentual_list)
    date_list = grouped_df['date'].astype(str).tolist()

    quantidade_list = grouped_df['quantidade'].tolist()

    return date_list,quantidade_list,variacao_percentual_list


def calcular_porcentagem_ids_unicos_pl(df1, df2, coluna_id='id'):

    coluna_id_sinistros = "policy_number"
    # Encontra os IDs únicos em cada DataFrame
    ids_unicos_df1 = df1.select(coluna_id_sinistros).unique()
    ids_unicos_df2 = df2.select(coluna_id).unique()

    # Calcula a diferença de conjuntos para encontrar IDs em df1 que não estão em df2
    len_df1 = len(ids_unicos_df1)
    len_df2 = len(ids_unicos_df2)
    diferenca = len_df2 - len_df1

    porcentagem = round(((diferenca/len_df2)*100),0)
    print(porcentagem)

    # print(ids_diferentes)
    # Calcula a porcentagem de IDs únicos em df1 em relação ao total de IDs únicos em df2

    return porcentagem


def calcular_porcentagem_notificationType_e_retornar_lista(df, coluna_notificationType='notificationType'):

    df = df.filter(pl.col('status_sinistro') == 'PENDENTE')

    # Conta a quantidade de cada tipo de notificação
    contagem = df.group_by(coluna_notificationType).agg([
        pl.len().alias('quantidade')
    ])

    # Calcula o total de notificações
    total_notificacoes = contagem['quantidade'].sum()

    # Calcula a porcentagem de cada tipo de notificação, arredonda o valor, e ordena pelo tipo de notificação
    porcentagem = contagem.with_columns(
        (pl.col('quantidade') / total_notificacoes * 100).round().cast(int).alias('porcentagem')
    ).sort(coluna_notificationType)

    # Convertendo o resultado em uma lista de tuplas
    resultado_lista = [(row[coluna_notificationType], f"{row['porcentagem']}") for row in porcentagem.to_dicts()]

    return resultado_lista

