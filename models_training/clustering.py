# salario medio poor ccaa
# Tasa de paeo por aa
# porcentaje de indefinidos
# precio vivienda 

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Herramientas de Machine Learning
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# CARGA Y PREPARACIÓN DE DATOS   
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
'''
Antes de comenzar, nos aseguraremos de que los datos están 
cargados correcatmente para útilizar aquellos que necesitamos
'''

dataframe = pd.read_csv('data_output/csv/ML.csv')    # cargar archivo
print("INFORMACIÓN DEL DATASET")
print(dataframe.info())
print("\nPRIMERAS 5 FILAS")
print(dataframe.head())

'''
INFORMACIÓN DEL DATASET
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 272 entries, 0 to 271
Data columns (total 5 columns):
 #   Column           Non-Null Count  Dtype
---  ------           --------------  -----
 0   anio             272 non-null    int64
 1   comunidad        272 non-null    object
 2   precio_vivienda  272 non-null    float64
 3   salario_medio    272 non-null    float64
 4   tasa_paro_media  272 non-null    float64
dtypes: float64(3), int64(1), object(1)
memory usage: 10.8+ KB

PRIMERAS 5 FILAS
   anio                comunidad  precio_vivienda  salario_medio  tasa_paro_media
0  2018                Andalucía        111.37050   21730.240000          22.9850
1  2012                Andalucía        106.26725   20770.716667          34.3525
2  2022  Asturias, Principado de        126.18175   26704.030000          12.5125
3  2019  Asturias, Principado de        110.56050   24882.073333          14.1925
4  2023               País Vasco        136.31625   33381.403333           7.7350
'''

# Apartaremos en un dataframe las variables que necesitamos para nuestro clustering
# Debemos escalar los datos ya que la tasa se paro es un porcentaje y el precio de 
# la vivienda y la tasa de paro está en euros

variables = ['salario_medio', 'precio_vivienda', 'tasa_paro_media']
datos = dataframe[variables]
datos_escalados = StandardScaler().fit_transform(datos)
dataframe_escalado = pd.DataFrame(datos_escalados, columns=variables)

print("\nDATOS SIN ESCALAR")
print(datos.head())

print("\nDATOS DESPUES DE ESCALAR")
print(dataframe_escalado.head())



# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ALGORITMO K-MEANS 
# Algoritmo de agrupamiento (clustering) más famoso del mundo   
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# Creación del algoritmo que buscará 3 cluster (grupos)
kmeans = KMeans(n_clusters=3, random_state=42)

# Entrenamiento
clusters_asignados = kmeans.fit_predict(datos_escalados)
dataframe['Cluster'] = clusters_asignados

print("\nRESULTADO DE LA INTELIGENCIA ARTIFICIAL")
print(dataframe[['comunidad', 'anio', 'salario_medio', 'Cluster']].head(10))