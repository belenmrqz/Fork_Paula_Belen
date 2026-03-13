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

ultimo_anio = dataframe['anio'].max() # Último año
df_foto = dataframe[dataframe['anio'] == ultimo_anio].copy() # dataframe con el último año

# Aislar variables y escalar
variables = ['salario_medio', 'precio_vivienda', 'tasa_paro_media']
datos = df_foto[variables]
datos_escalados = StandardScaler().fit_transform(datos)



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 2. ALGORITMO K-MEANS
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
kmeans = KMeans(n_clusters=3, random_state=42)
df_foto['Cluster'] = kmeans.fit_predict(datos_escalados)



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 3. INTELIGENCIA DE COLORES Y FORMATO PARA LA GRÁFICA
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
estadisticas = df_foto.groupby('Cluster')[['salario_medio', 'tasa_paro_media']].mean()
estadisticas['Puntuacion'] = estadisticas['salario_medio'] - (estadisticas['tasa_paro_media'] * 500)
estadisticas = estadisticas.sort_values(by='Puntuacion', ascending=False)

grupo_verde = estadisticas.index[0]
grupo_amarillo = estadisticas.index[1]
grupo_rojo = estadisticas.index[2]

diccionario_nombres = {
    grupo_verde: '🟢 Óptima (Alto Salario, Bajo Paro)',
    grupo_amarillo: '🟡 Intermedia (Transición)',
    grupo_rojo: '🔴 Vulnerable (Bajo Salario, Alto Paro)'
}

mapa_colores = {
    '🟢 Óptima (Alto Salario, Bajo Paro)': '#2ca02c', 
    '🟡 Intermedia (Transición)': '#ffc107',          
    '🔴 Vulnerable (Bajo Salario, Alto Paro)': '#d62728' 
}

df_foto['Estado Financiero'] = df_foto['Cluster'].map(diccionario_nombres)

# Textos para el hover 
df_foto['Salario'] = df_foto['salario_medio'].round(0).astype(int).astype(str) + " €"
df_foto['Paro'] = df_foto['tasa_paro_media'].round(1).astype(str) + " %"
df_foto['Índice Vivienda'] = df_foto['precio_vivienda'].round(1).astype(str) + " (Base 100)"



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 4. PREPARAR NOMBRES PARA EL MAPA 
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Este es el mapeo EXACTO que usa el archivo geojson de click_that_hood
diccionario_ccaa = {
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
df_foto['Nombre_Mapa'] = df_foto['comunidad'].map(diccionario_ccaa)



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 5. CREAR EL GRÁFICO HTML INTERACTIVO
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
url_mapa = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/spain-communities.geojson"

fig = px.choropleth_map(
    df_foto,
    geojson=url_mapa, 
    featureidkey="properties.name", 
    locations="Nombre_Mapa",        
    color="Estado Financiero",
    color_discrete_map=mapa_colores,
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
    title=f"Radiografía del Poder Adquisitivo en España ({ultimo_anio})<br><sub>Agrupación K-Means de Comunidades Autónomas.</sub>"
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
carpeta_salida = r"C:\Users\BelenML\github\SBD_Act1.1\data_output\graphics\clustering_graphics"
os.makedirs(carpeta_salida, exist_ok=True)
archivo_salida = os.path.join(carpeta_salida, 'mapa_interactivo_ccaa.html')

fig.write_html(archivo_salida)

print(f"Mapa de regiones creado con éxito en {archivo_salida} ")
