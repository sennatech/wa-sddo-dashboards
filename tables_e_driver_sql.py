import datetime
import pyodbc
import textwrap
import queries
import funcoes as fc

import pandas as pd
from credentials import username, password
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
''')
cnxn: pyodbc.Connection = pyodbc.connect(connection_string)
crsr: pyodbc.Cursor = cnxn.cursor()

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
