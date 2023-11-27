import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects
import plotly.graph_objects as go
from dash import Dash, html, dcc
import tables_e_driver_sql as tb
import dash_bootstrap_components as dbc
import json
brazil_states = json.load(open("venv/brazil_geo.json", "r"))
table_mapa_emissoes = tb.table_emissoes_unica[["insured_address_state","policy_amount","coverage_sum_insured","eventtime"]]
table_mapa_emissoes_bh = table_mapa_emissoes[table_mapa_emissoes["insured_address_state"] == "BH"]
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])

# fig = px.choropleth_mapbox(table_mapa_emissoes,locations="insured_address_state", color="policy_amount",
#                            center={"lat": -16.45, "lon": -46.35}, zoom=4,
#                            geojson=brazil_states,color_continuous_scale="Redor",opacity=0.4,
#                            hover_data={"coverage_sum_insured": True})
# fig.update_layout(
#     paper_bgcolor ="#242424",
#     autosize = True,
#     margin=dict(l=10,r=10,t=10,b=10),
#     showlegend= False,
#     mapbox_style = "carto-darkmatter"
# )

fig2 = go.Figure(layout={"template": "plotly_dark"})
fig2.add_trace(go.Scatter(x=table_mapa_emissoes_bh["eventtime"],y=table_mapa_emissoes_bh["coverage_sum_insured"]))
fig2.update_layout(
    paper_bgcolor="#242424",
    plot_bgcolor = "#242424",
    autosize = True,
    margin = dict(l=10,r=10,t=10,b=10)

)
app.layout = dbc.Container(
    dbc.Row([
        dbc.Col([
            html.Div([
                html.Img(id="logo",src=app.get_asset_url("SDDO.png"),height=50),
                html.H5("Teste para o uso de pyhton no SDDO"),
                dbc.Button("Brasil",color="#6AF4FF", id = "location-button", size= "lg")
            ], style={}),
            dcc.Graph(id="line_graph", figure=fig2)
        ]),
        # dbc.Col([
        #     dcc.Graph(id="cloroplhepth-map",figure=fig)
        # ])
    ])
)

if __name__ == "__main__":
    app.run_server(debug=True)


