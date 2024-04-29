import funcoes_e_driver as fc
import polars as pl


# Queries SQL
def retorna_dados(cpfs, estados, causas):
    cpfs = tuple(cpfs)
    estados = tuple(estados)
    causas = tuple(causas)

    select_sql_sinistros = f"""
    SELECT  * FROM sddo.sinistros
    WHERE document_number IN {cpfs}
    AND state IN {estados}
    AND notificationType IN {causas}
    """
    # print(f"sql sinisstros {select_sql_sinistros}")

    select_sql_emissoes = f"""
    SELECT  * FROM sddo.emissoes
    WHERE document_number IN {cpfs}
    AND holder_address_state IN {estados}
    AND coverage_name IN {causas}
    """

    # print(f"sql emissao  {select_sql_emissoes}")


    select_sql_cotacoes = f"""
    SELECT  * FROM sddo.cotacoes
    WHERE document_number IN {cpfs}
    AND address_state IN {estados}
    AND coverage IN {causas}
    """
    # print(f"sql cotcao  {select_sql_cotacoes}")


    select_view_tempo_medio = "SELECT * FROM dbo.vw_sinistros_diferenca_eventtime"
    # select_view_sinistros_unica = "SELECT * FROM dbo.vw_sinistros_status_atual"
    # select_view_emissoes_unica = "SELECT * FROM dbo.vw_emissoes_combinada"
    # select_view_cotacoes_unica = "SELECT * FROM dbo.vw_cotacoes_combinada"

    table_sinistro = fc.transforma_query_em_sql(select_sql_sinistros)
    table_emissoes = fc.transforma_query_em_sql(select_sql_emissoes)
    table_cotacoes = fc.transforma_query_em_sql(select_sql_cotacoes)
    # table_cotacoes_unica = fc.transforma_query_em_sql(select_view_cotacoes_unica)
    # table_sinistros_unica = (fc.transforma_query_em_sql(select_view_sinistros_unica)).sort("date")
    # table_emissoes_unica = fc.transforma_query_em_sql(select_view_emissoes_unica)
    table_sinistro_tempo_medio = fc.transforma_query_em_sql(select_view_tempo_medio)

    # sinistros_aprovados = table_sinistros_unica.filter(pl.col('status_sinistro') == 'APROVADO')
    # sinistros_recusados = table_sinistros_unica.filter(pl.col('status_sinistro') == 'RECUSADO')
    # sinistros_em_aberto = table_sinistros_unica.filter(pl.col('status_sinistro') == 'PENDENTE')
    # sinistros_fechados = table_sinistros_unica.filter(pl.col('status_sinistro') != 'PENDENTE')

    tempo_medio_resposta = round(table_sinistro_tempo_medio["resultado_da_diferenca"].mean())
    ticket_medio = round(table_emissoes["amount"].mean())
    ticket_medio_policy = round(table_emissoes["amount"].mean())
    num_cotacoes = table_cotacoes.shape[0]
    preco_medio_cotação = round(table_cotacoes["amount"].sum() / num_cotacoes, 2)
    apolices_ativas = table_emissoes.shape[0]
    return (table_sinistro, table_emissoes, table_cotacoes,
            table_sinistro_tempo_medio, tempo_medio_resposta, ticket_medio, ticket_medio_policy, preco_medio_cotação,
            apolices_ativas)
