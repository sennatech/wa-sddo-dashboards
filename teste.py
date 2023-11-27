import json

import pandas as pd

import  tables_e_driver_sql as tb
brazil_states = json.load(open("venv/brazil_geo.json", "r"))

print(brazil_states["features"][0].keys())
print(brazil_states["features"][0]["id"])

table_mapa_emissoes = tb.table_emissoes_unica[["insured_address_state","policy_amount","coverage_sum_insured","eventtime"]]
print(type(table_mapa_emissoes["eventtime"][0]))
# brazil_states["geometry"] = brazil_states["features"]["geometry"]
# x= pd.DataFrame(brazil_states["features"])
