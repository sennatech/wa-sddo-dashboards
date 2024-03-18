import textwrap
import pyodbc
import polars as pl
from credentials import username, password



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
    eventos_por_tempo = dataframe.groupby('date').count().rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    eventos_por_tempo_pandas['variação_percentual'] = eventos_por_tempo_pandas['quantidade'].pct_change().mul(100).round(0)
    eventos_por_tempo_pandas['variação_percentual'] = eventos_por_tempo_pandas['quantidade'].pct_change().mul(100).round(0).fillna(0)

    variacao_percentual_list = eventos_por_tempo_pandas['variação_percentual'].tolist()
    if pd.isna(variacao_percentual_list[0]):
        variacao_percentual_list[0] = 0
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
    status_counts = df_sinistro_filtrada.groupby("status_sinistro").agg(pl.count().alias('count'))
    # Converter Polars DataFrame para Pandas DataFrame para usar com Plotly
    status_counts_df = status_counts.to_pandas()

    # Filtrar e extrair os valores diretamente
    recusado_row = status_counts.filter(pl.col('status_sinistro') == 'RECUSADO').select('count')
    pendente_row = status_counts.filter(pl.col('status_sinistro') == 'PENDENTE').select('count')
    aprovado_row = status_counts.filter(pl.col('status_sinistro') == 'APROVADO').select('count')

    # Verificar se os DataFrames filtrados estão vazios e extrair o valor ou atribuir 0
    recusado_val = recusado_row['count'][0] if not recusado_row.is_empty() else 0
    pendente_val = pendente_row['count'][0] if not pendente_row.is_empty() else 0
    aprovado_val = aprovado_row['count'][0] if not aprovado_row.is_empty() else 0

    print(f"Recusado: {recusado_val}, Pendente: {pendente_val}, Aprovado: {aprovado_val}")

    return recusado_val,pendente_val,aprovado_val

def retorna_sinistro_por_estado(df_sinistro_filtrado):

    agrupado_por_estado = df_sinistro_filtrado.groupby("state").agg(pl.count().alias('count'))
    agrupado_por_estado_pandas = agrupado_por_estado.to_pandas()
    agrupado_por_estado_pandas.sort_values(by='count', inplace=True)
    state_list = agrupado_por_estado_pandas['state'].tolist()
    count_list = agrupado_por_estado_pandas['count'].tolist()

    return state_list,count_list


import pandas as pd


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
    if pd.isna(variacao_percentual_list[0]):
        variacao_percentual_list[0] = 0
    quantidade_list = grouped_df['quantidade'].tolist()

    return date_list,quantidade_list,variacao_percentual_list

def retorna_valores_quantidade_por_tempo_emissao(dataframe_emissao):
    eventos_por_tempo = dataframe_emissao.groupby('eventtime').count().rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    eventos_por_tempo_pandas['eventtime'] = pd.to_datetime(eventos_por_tempo_pandas['eventtime'])
    eventos_por_tempo_pandas['date'] = eventos_por_tempo_pandas['eventtime'].dt.date
    grouped_df = eventos_por_tempo_pandas.groupby('date')['quantidade'].sum().reset_index()
    grouped_df['variação_percentual'] = grouped_df['quantidade'].pct_change().mul(100).round(0)
    grouped_df['variação_percentual'] = grouped_df['quantidade'].pct_change().mul(100).round(0).fillna(0)
    variacao_percentual_list = grouped_df['variação_percentual'].tolist()
    print(variacao_percentual_list)
    date_list = grouped_df['date'].astype(str).tolist()
    if pd.isna(variacao_percentual_list[0]):
        variacao_percentual_list[0] = 0
    quantidade_list = grouped_df['quantidade'].tolist()

    return date_list,quantidade_list,variacao_percentual_list
