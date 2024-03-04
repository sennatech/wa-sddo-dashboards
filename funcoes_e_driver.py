import textwrap
import pyodbc
import math
import polars as pl
from credentials import username, password
import plotly.express as px
import plotly.graph_objects as go


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
    "MultipleActiveResultSets=True"
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



def plotar_grafico_barras_pl(dataframe, titulo):
    eventos_por_tempo = dataframe.groupby('date').count().rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    fig = go.Figure()
    print(eventos_por_tempo_pandas)
    # Add bars with the specific colors you've requested
    fig.add_trace(go.Bar(
        x=eventos_por_tempo_pandas['date'],
        y=eventos_por_tempo_pandas['quantidade'],
        marker=dict(color="black")
    ))

    # Update the layout with the requested colors and settings
    fig.update_layout(
        margin=dict(t=30, b=30, l=30, r=30),
        paper_bgcolor='#333962',
        plot_bgcolor='#333962',
        font=dict(color='white'),
        title_font=dict(size=18),
        xaxis=dict(
            tickmode='array',
            tickvals=eventos_por_tempo_pandas['date'],
            ticktext=[date.strftime('%b %d') for date in eventos_por_tempo_pandas['date']],
            showline=True,
            linecolor='white',
            linewidth=2,
            ticks='outside',
            tickfont=dict(
                family='Arial',
                size=12,
                color='white'
            )
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='#485082'
        )
    )

    # Set y-axis range to be 20% higher than the max 'quantidade' value
    max_quantity = eventos_por_tempo_pandas['quantidade'].max()
    fig.update_yaxes(range=[0, max_quantity * 1.2])

    return fig

def plotar_grafico_pizza(df1, df2,label1,label2, titulo):

    num_cotacoes = df1.shape[0]
    num_emissoes = df2.shape[0]
    labels = [label1, label2]

    fig = px.pie(names=labels, values=[num_cotacoes, num_emissoes])
    fig.update_layout(
        margin=dict(t=30, b=10, l=0, r=0),
        showlegend=False,
        paper_bgcolor='white',
        font=dict(color='black'),
        title_font=dict(size=18)
    )
    fig.update_traces(marker=dict(colors=['#30679A', '#31999A'], line=dict(color='rgb(17,17,17)', width=2)))
    return fig


def create_donut_chart_from_csv(df_sinistro_filtrada):


    # Calcular a contagem de cada valor na coluna especificada
    status_counts = df_sinistro_filtrada.groupby("status_sinistro").agg(pl.count().alias('count'))
    # Converter Polars DataFrame para Pandas DataFrame para usar com Plotly
    status_counts_df = status_counts.to_pandas()
    # Certifique-se de que os nomes das colunas estão corretos aqui
    labels = status_counts_df["status_sinistro"]
    values = status_counts_df['count']  # Usando 'count' como definido pelo alias na agregação
    status_colors = {
        'RECUSADO': '#690000',
        'APROVADO': '#70BF7B',
        'PENDENTE': '#C2B70C'
    }
    colors = [status_colors.get(status, 'gray') for status in status_counts_df["status_sinistro"]]

    # Criar um gráfico de pizza (rosca) com um buraco no meio usando Plotly
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.5, marker=dict(colors=colors))])
    fig.update_traces(textinfo='percent+label')
    fig.update_layout(
        margin=dict(t=25, b=5, l=0, r=0),
        showlegend=False,
        paper_bgcolor='#FFFFFF',
        font=dict(color='black'),
        title_font=dict(size=18)
    )
    return fig
def cria_grafico_rosquinha_genero(df_filtrado_sinistros,label1,label2):
    # Contagem de sinistros por gênero usando Polars

    df_sinistros_M = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'M')
    df_sinistros_F = df_filtrado_sinistros.filter(pl.col('genderNotifier') == 'F')
    num_sinistros_M = df_sinistros_M.shape[0]
    num_sinistros_F = df_sinistros_F.shape[0]
    # Convertendo para listas Python para uso no Plotly
    labels = [label1,label2]
    values = [num_sinistros_M,num_sinistros_F]
    colors = ['#31999A', '#30679A']

    # Criando o gráfico de rosquinha
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.5,marker=dict(colors=colors))])

    fig.update_layout(
        margin=dict(t=25, b=5, l=0, r=0),
        showlegend=False,
        paper_bgcolor='#FFFFFF',
        font=dict(color='black'),
        title_font=dict(size=18)
    )

    return fig
def retorna_valores_quantidade_por_tempo(dataframe):
    eventos_por_tempo = dataframe.groupby('date').count().rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    return  eventos_por_tempo_pandas["date"],eventos_por_tempo_pandas["quantidade"]

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