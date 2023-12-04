import datetime
import pyodbc
import textwrap
import queries
import funcoes as fc
import funcoes1 as fc1
import pandas as pd


table_sinistros = fc.transforma_query_em_sql(queries.select_sql_sinistros)
table_emissoes = fc.transforma_query_em_sql(queries.select_sql_emissoes)
table_cotacoes = fc.transforma_query_em_sql(queries.select_sql_cotacoes)
table_pagamentos = fc.transforma_query_em_sql(queries.select_sql_pagamentos)
table_coberturas = fc.transforma_query_em_sql(queries.select_sql_coberturas)
table_clientes = fc.transforma_query_em_sql(queries.select_sql_clientes)
table_usuarios = fc.transforma_query_em_sql(queries.select_sql_usuarios)
table_emissoes_unica = pd.DataFrame(fc.retira_duplicadas_calcula_validade_policy_emissao(table_emissoes)).reset_index()
table_sinistros_unica = pd.DataFrame(fc.retira_duplicadas_sinistro(table_sinistros)).reset_index()
df_soma_por_estado = pd.DataFrame(table_emissoes.groupby('insured_address_state')['policy_amount'].sum().reset_index())
# table_mapa_emissoes = table_emissoes_unica.columns = (["insured_address_state","policy_amount","coverage_sum_insured"])
select_columns = fc.tipos_de_notificacao(table_sinistros_unica)
select_columns = [select_columns]
for i in select_columns:
    [select_columns_causa_sinistro] = select_columns

state_unico = table_sinistros_unica.drop_duplicates(subset="state")
state_unico = state_unico["state"]
media_resp_sinistro = fc.calcula_tempo_medio_aprovacao_sinistro(table_sinistros)
# print(media_resp_sinistro)