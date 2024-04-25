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
    with get_connection() as cnxn:
        # Execute the SQL query and fetch the result
        crsr = cnxn.cursor()
        df = pl.read_database(query=sql_query, connection=crsr)
        crsr.close()
    return df

def retorna_valores_quantidade_por_tempo_sinistro(dataframe):
    eventos_por_tempo = dataframe.groupby('date').agg(pl.count().alias('quantidade'))
    eventos_por_tempo = eventos_por_tempo.sort('date')  # Garantindo que está ordenado por data

    # Calculando a variação percentual
    quantidade_shifted = eventos_por_tempo['quantidade'].shift(-1).fill_null(0)
    variação_percentual = (quantidade_shifted / eventos_por_tempo['quantidade'] - 1) * 100

    eventos_por_tempo = eventos_por_tempo.with_columns(
        variação_percentual.round().alias('variação_percentual')
    )

    return eventos_por_tempo



def retorna_valores_genero(df_filtrado_sinistros):
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
    status_counts = df_sinistro_filtrada.groupby("status_sinistro").agg(pl.count().alias('count'))
    total_sinistros = df_sinistro_filtrada.shape[0]
    recusado_val = status_counts.filter(pl.col('status_sinistro') == 'RECUSADO').select('count').get_column('count').get_first() or 0
    pendente_val = status_counts.filter(pl.col('status_sinistro') == 'PENDENTE').select('count').get_column('count').get_first() or 0
    aprovado_val = status_counts.filter(pl.col('status_sinistro') == 'APROVADO').select('count').get_column('count').get_first() or 0
    return calc_status_stats(recusado_val, pendente_val, aprovado_val, total_sinistros)

def calc_status_stats(recusado, pendente, aprovado, total):
    porcentagem_recusado = round((recusado / total) * 100, 0) if total else 0
    porcentagem_pendente = round((pendente / total) * 100, 0) if total else 0
    porcentagem_aprovado = round((aprovado / total) * 100, 0) if total else 0
    sinistros_fechados = recusado + aprovado
    sinistro_em_aberto = pendente
    porcentagem_sinistros_aberto = round((sinistro_em_aberto / total) * 100, 0) if total else 0
    porcentagem_sinistros_fechado = round((sinistros_fechados / total) * 100, 0) if total else 0
    return (recusado, pendente, aprovado, porcentagem_recusado, porcentagem_pendente, porcentagem_aprovado, porcentagem_sinistros_aberto, porcentagem_sinistros_fechado)

def retorna_sinistro_por_estado(df_sinistro_filtrado):
    agrupado_por_estado = df_sinistro_filtrado.groupby("state").agg(pl.count().alias('count'))
    agrupado_por_estado = agrupado_por_estado.sort('count')
    return agrupado_por_estado

def retorna_valores_quantidade_por_tempo_cotacao(dataframe_cotacao):
    return retorna_valores_quantidade_por_tempo_sinistro(dataframe_cotacao)  # Reutilizando a função com a lógica aplicada para cotacoes

def retorna_valores_quantidade_por_tempo_emissao(dataframe_emissao):
    return retorna_valores_quantidade_por_tempo_sinistro(dataframe_emissao)  # Reutilizando a função com a lógica aplicada para emissao
