# salario medio poor ccaa
# Tasa de paeo por aa
# porcentaje de indefinidos
# precio vivienda 

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import plotly.express as px
import os

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 1. CARGA Y PREPARACIÓN DE DATOS
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
dataframe = pd.read_csv('data_output/csv/ML.csv')

last_year = dataframe['anio'].max() # Último año
df_snapshot = dataframe[dataframe['anio'] == last_year].copy() # dataframe con el último año

# Aislar variables y escalar
features = ['salario_medio', 'precio_vivienda', 'tasa_paro_media']
data = df_snapshot[features]
scaled_data = StandardScaler().fit_transform(data)



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 2. ALGORITMO K-MEANS
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
kmeans = KMeans(n_clusters=3, random_state=42)
df_snapshot['Cluster'] = kmeans.fit_predict(scaled_data)



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 3. INTELIGENCIA DE COLORES Y FORMATO PARA LA GRÁFICA
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
stats = df_snapshot.groupby('Cluster')[['salario_medio', 'tasa_paro_media']].mean()
stats['Puntuacion'] = stats['salario_medio'] - (stats['tasa_paro_media'] * 500)
stats = stats.sort_values(by='Puntuacion', ascending=False)

green_cluster = stats.index[0]
yellow_cluster = stats.index[1]
red_cluster = stats.index[2]

name_dict = {
    green_cluster: '🟢 Óptima (Alto Salario, Bajo Paro)',
    yellow_cluster: '🟡 Intermedia (Transición)',
    red_cluster: '🔴 Vulnerable (Bajo Salario, Alto Paro)'
}

color_map = {
    '🟢 Óptima (Alto Salario, Bajo Paro)': '#2ca02c', 
    '🟡 Intermedia (Transición)': '#ffc107',          
    '🔴 Vulnerable (Bajo Salario, Alto Paro)': '#d62728' 
}

df_snapshot['Estado Financiero'] = df_snapshot['Cluster'].map(name_dict)

# Textos para el hover 
df_snapshot['Salario'] = df_snapshot['salario_medio'].round(0).astype(int).astype(str) + " €"
df_snapshot['Paro'] = df_snapshot['tasa_paro_media'].round(1).astype(str) + " %"
df_snapshot['Índice Vivienda'] = df_snapshot['precio_vivienda'].round(1).astype(str) + " (Base 100)"



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 4. PREPARAR NOMBRES PARA EL MAPA 
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Este es el mapeo EXACTO que usa el archivo geojson de click_that_hood
region_dict = {
    'Andalucía': 'Andalucia',
    'Aragón': 'Aragon',
    'Asturias, Principado de': 'Asturias', 
    'Balears, Illes': 'Baleares',
    'Canarias': 'Canarias',
    'Cantabria': 'Cantabria',
    'Castilla y León': 'Castilla-Leon',          
    'Castilla - La Mancha': 'Castilla-La Mancha',
    'Cataluña': 'Cataluña',                       
    'Comunitat Valenciana': 'Valencia',           
    'Extremadura': 'Extremadura',
    'Galicia': 'Galicia',
    'Madrid, Comunidad de': 'Madrid',
    'Murcia, Región de': 'Murcia',
    'Navarra, Comunidad Foral de': 'Navarra', 
    'País Vasco': 'Pais Vasco',                   
    'Rioja, La': 'La Rioja'
}
df_snapshot['Nombre_Mapa'] = df_snapshot['comunidad'].map(region_dict)



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 5. CREAR EL GRÁFICO HTML INTERACTIVO
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
map_url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/spain-communities.geojson"

fig = px.choropleth_map(
    df_snapshot,
    geojson=map_url, 
    featureidkey="properties.name", 
    locations="Nombre_Mapa",        
    color="Estado Financiero",
    color_discrete_map=color_map,
    hover_name="comunidad",
    hover_data={
        "Nombre_Mapa": False, 
        "Estado Financiero": False, 
        "Salario": True, 
        "Paro": True,
        "precio_vivienda": False, 
        "Índice Vivienda": True          
    },
    map_style="carto-positron", 
    zoom=4.8, 
    center={"lat": 40.0, "lon": -3.0}, 
    title=f"Radiografía del Poder Adquisitivo en España ({last_year})<br><sub>Agrupación K-Means de Comunidades Autónomas.</sub>"
)

# Ajustes visuales
fig.update_layout(
    margin={"r":0,"t":80,"l":0,"b":0},
    title_font_size=20,
    title_x=0.5, 
    legend_title_text='Poder Adquisitivo Real',
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=0.02,
        xanchor="center",
        x=0.5,
        bgcolor="rgba(255, 255, 255, 0.9)",
        bordercolor="Black", 
        borderwidth=1
    )
)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 6. GUARDAR EL GRÁFICO  
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
fig.write_html("data_output/graphics/clustering_graphics/mapa_interactivo_por_ccaa.html")

print(f"Mapa de regiones creado con éxito")
