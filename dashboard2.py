import funcoes as fc
import tables_e_driver_sql as tb
import queries as queries
import streamlit as st
import pandas as pd
import plotly.express as px
import main as main
def plotar_grafico_pizza(cotacao,emissoes,titulo):
    num_cotacoes = cotacao.shape[0]
    num_emissoes = emissoes.shape[0]

    fig = px.pie( values=[num_cotacoes,num_emissoes] ,title= titulo)

    # Exiba o gráfico
    return fig

table_cotacoes = fc.transforma_query_em_sql(queries.select_sql_cotacoes)
num_cotacoes = table_cotacoes.shape[0]
table_emissoes_unica = pd.DataFrame(fc.retira_duplicadas_calcula_validade_policy_emissao(fc.transforma_query_em_sql(queries.select_sql_emissoes))).reset_index()
num_emissoes = table_emissoes_unica.shape[0]
table_sinistros_unica = table_sinistros_unica = pd.DataFrame(fc.retira_duplicadas_sinistro(fc.transforma_query_em_sql(queries.select_sql_sinistros))).reset_index()
sinistros_em_aberto = table_sinistros_unica[table_sinistros_unica["status_sinistro"] == "PENDENTE"]
media_resp_sinistro = fc.calcula_tempo_medio_aprovacao_sinistro(table_sinistros_unica)
apolices_ativas = table_emissoes_unica.shape[0]
ticket_medio = (table_emissoes_unica["issuance_amount"].sum()/num_emissoes)
# sinistros_avisados = plotar_grafico_barras(table_sinistros_unica)
st.set_page_config(layout="wide")
table_sinistros_unica = table_sinistros_unica.sort_values("date")
table_sinistros_unica["Month"] = table_sinistros_unica['date'].apply(lambda x: str(x.year) + "-" + str(x.month))
print(sinistros_em_aberto)
causa = st.sidebar.selectbox("causa", table_sinistros_unica["notificationType"].unique())
df_filtrado  = table_sinistros_unica[table_sinistros_unica["notificationType"] == causa ]
df_filtrado_pendente = sinistros_em_aberto[sinistros_em_aberto["notificationType"] == causa ]
col1, col2, col3, col4 = st.columns(4)
col5, col6, col7  = st.columns(3)
fig_causa_geral = main.plotar_grafico_barras(df_filtrado,"Quantidade de sinistros  por Tempo")
fig_causa_pendente = main.plotar_grafico_barras(df_filtrado_pendente,'Quantidade de sinistros pendentes por Tempo')
fig_cotacoes_emissoes = plotar_grafico_pizza(table_cotacoes,table_emissoes_unica,"Cotacoes X Emissoes")
col1.metric(label="TicketMêdio", value=ticket_medio)
col2.metric(label="Sinistros em Aberto", value=sinistros_em_aberto.shape[0])
col3.metric(label="Tempo Medio de Resposta", value=media_resp_sinistro)
col4.metric(label="Apolices Ativas", value=apolices_ativas)
col5.plotly_chart(fig_causa_geral)
col6.plotly_chart(fig_causa_pendente)
col7.plotly_chart(fig_cotacoes_emissoes)
