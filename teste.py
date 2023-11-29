import json

import pandas as pd

import  tables_e_driver_sql as tb
# brazil_states = json.load(open("venv/brazil_geo.json", "r"))
#
# print(brazil_states["features"][0].keys())
# print(brazil_states["features"][0]["id"])
#
# table_mapa_emissoes = tb.table_emissoes_unica[["insured_address_state","policy_amount","coverage_sum_insured","eventtime"]]
# print(type(table_mapa_emissoes["eventtime"][0]))
# # brazil_states["geometry"] = brazil_states["features"]["geometry"]
# # x= pd.DataFrame(brazil_states["features"])
# fig = px.choropleth_mapbox(table_mapa_emissoes,locations="insured_address_state", color="policy_amount",
#                            center={"lat": -16.45, "lon": -46.35}, zoom=4,
#                            geojson=brazil_states,color_continuous_scale="Redor",opacity=0.4,
#                            hover_data={"coverage_sum_insured": True})


# dbc.Col([
#     dcc.Graph(id="cloroplhepth-map",figure=fig)
# ])
# print(tb.select_columns)
# select_columns = [tb.select_columns]
# for i in select_columns:
#     [select_columns_1] = select_columns
# print(select_columns_1[0])

print(tb.media_resp_sinistro)
