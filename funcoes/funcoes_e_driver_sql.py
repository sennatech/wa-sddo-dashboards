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
    eventos_por_tempo = dataframe.groupby('date').count()
    eventos_por_tempo = eventos_por_tempo.rename({"date": "data"})
    eventos_por_tempo = eventos_por_tempo.rename({"count": "quantidade"})
    eventos_por_tempo_pandas = eventos_por_tempo.to_pandas()
    fig = px.bar(eventos_por_tempo_pandas, x='data', y='quantidade', title=titulo)
    fig.update_layout(
        margin=dict(t=30, b=30, l=30, r=30),
        paper_bgcolor='#001322',
        plot_bgcolor='#001322',
        font=dict(color='white'),
        title_font=dict(size=18),
        xaxis=dict(
            showline=True,
            showgrid=True,
            showticklabels=True,
            linecolor='white',
            linewidth=2,
            ticks='outside',
            tickfont=dict(
                family='Arial',
                size=12,
                color='white'
            ),
            # To ensure every date shows up, we'll use the tickmode='array' and tickvals set to the date range.
            tickmode='array',
            tickvals=eventos_por_tempo_pandas['data'],
            ticktext=[date.strftime('%b %d') for date in eventos_por_tempo_pandas['data']]
        ),
    )
    print(eventos_por_tempo_pandas)
    fig.update_yaxes(range=[0, max(eventos_por_tempo_pandas['quantidade']) * 1.2])
    fig.update_traces(marker_color='#636EFA')
    return fig

def plotar_grafico_pizza(df1, df2,label1,label2, titulo):

    num_cotacoes = df1.shape[0]
    num_emissoes = df2.shape[0]
    labels = [label1, label2]

    fig = px.pie(names=labels, values=[num_cotacoes, num_emissoes], title=titulo)
    fig.update_layout(
        margin=dict(t=25, b=0, l=0, r=0),
        showlegend=False,
        paper_bgcolor='#001322',
        font=dict(color='white'),
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
    fig.update_layout(title_text=f'Distribuição dos valores para status_sinistro')
    fig.update_layout(
        margin=dict(t=25, b=5, l=0, r=0),
        showlegend=False,
        paper_bgcolor='#FFFFFF',
        font=dict(color='black'),
        title_font=dict(size=18)
    )
    return fig
