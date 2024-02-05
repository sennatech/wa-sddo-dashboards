import textwrap
import pyodbc
import math
import polars as pl
from credentials import username, password
import plotly.express as px

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
        paper_bgcolor='rgb(17,17,17)',
        plot_bgcolor='rgb(17,17,17)',
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
        paper_bgcolor='rgb(17,17,17)',
        font=dict(color='white'),
        title_font=dict(size=18)
    )
    fig.update_traces(marker=dict(colors=['#30679A', '#31999A'], line=dict(color='rgb(17,17,17)', width=2)))
    return fig