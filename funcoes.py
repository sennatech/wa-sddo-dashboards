import tables_e_driver_sql as csq
import pandas as pd
import datetime
def transforma_query_em_sql(sql_query):
    csq.crsr.execute(sql_query)
    colunas = [i[0] for i in csq.crsr.description]
    dados = csq.crsr.fetchall()
    data = [tuple(rows) for rows in dados]
    df = pd.DataFrame(data, columns=colunas)
    return df

def retira_duplicadas_sinistro(dataframe_sinistros):
    database_sorted = dataframe_sinistros.sort_values(by='status_sinistro')
    mask = database_sorted.duplicated(subset='id') & (database_sorted['status_sinistro'] == "PENDENTE")
    rows_to_drop = database_sorted.index[mask]
    filtered_dataset = database_sorted.drop(rows_to_drop)
    return filtered_dataset

# def retira_duplicadas_emissao_e_diferenca_dias(dataframe_emissoes):
#     data_atual = datetime.date.today()
#     print(data_atual)
#     dataframe_emissoes['policy_validity_end'] = pd.to_datetime(dataframe_emissoes['policy_validity_end'])
#     print(dataframe_emissoes['policy_validity_end'])
#     dataframe_emissoes['diferenca_dias'] = (data_atual - dataframe_emissoes['policy_validity_end']).dt.days
#     print(dataframe_emissoes['diferenca_dias'])
#     dataset_sorted = dataframe_emissoes.sort_values(by='policy_number')
#     mask = dataset_sorted.duplicated(subset='policy_number')
#     print(mask)
#     rows_to_drop = dataset_sorted.index[mask]
#     filtered_dataset = dataset_sorted.drop(rows_to_drop)
#     filtered_dataset = filtered_dataset.reset_index(drop=True, inplace=True)
#     return filtered_dataset

def retira_duplicadas_calcula_validade_policy_emissao(dataset):
    data_atual = datetime.datetime.now()
    data_fim_politica = pd.to_datetime(dataset["policy_validity_end"], format='%Y-%m-%d')
    diferenca_dias = (data_atual - data_fim_politica).dt.days
    # dataset_sorted = dataset.sort_values(by='policy_number')
    dataset_sorted = dataset.sort_values(by='policy_validity_end')
    mask = dataset_sorted.duplicated(subset='policy_number')
    rows_to_drop = dataset_sorted.index[mask]
    filtered_dataset = dataset_sorted.drop(rows_to_drop)
    filtered_dataset["diferenca_dias"] = diferenca_dias
    return filtered_dataset

def calcula_tempo_medio_aprovacao_sinistro(dataframe_sinistros):
    dataframe_sinistros['eventtime'] = pd.to_datetime(dataframe_sinistros['eventtime'])
    if 'eventtime' not in dataframe_sinistros.columns or 'id' not in dataframe_sinistros.columns or 'status_sinistro' not in dataframe_sinistros.columns:
        raise ValueError("O dataset deve conter colunas 'id', 'eventtime' e 'status_sinistro'.")
    pendentes = dataframe_sinistros[dataframe_sinistros['status_sinistro'] == 'PENDENTE']
    aprovados = dataframe_sinistros[dataframe_sinistros['status_sinistro'] == 'APROVADO']
    merge_result = pd.merge(pendentes, aprovados, on='id', suffixes=('_pendente', '_aprovado'))
    merge_result['diferenca_eventtime'] = (merge_result['eventtime_aprovado'] - merge_result['eventtime_pendente']).dt.total_seconds()
    merge_result['diferenca_eventtime'] /= 3600
    media_diferenca_eventtime = merge_result['diferenca_eventtime'].mean()
    media_arredondada = round(media_diferenca_eventtime,3)
    return media_arredondada

