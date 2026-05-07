import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# ── FUNCIONES DE CARGA ─────────────────────────────────────────────────────
@st.cache_data
def load_ml_data():
    return pd.read_csv("data_output/csv/ML.csv")

@st.cache_data
def load_geojson():
    url = "https://raw.githubusercontent.com/R-CoderDotCom/data/main/shapefile_spain/spain.geojson"
    return requests.get(url).json()

# ── VISTA PRINCIPAL DEL CLUSTERING ─────────────────────────────────────────
def show_clustering():
    try:
        df_ml_raw = load_ml_data()
        ultimo_anio = df_ml_raw['anio'].max()
        df_ml = df_ml_raw[df_ml_raw['anio'] == ultimo_anio].copy()

    except Exception as e:
        st.error(f"Error cargando los datos de ML: {e}")
        return

    # --- PREPARACIÓN Y ENTRENAMIENTO DEL MODELO ---
    try:
        features = ['salario_medio', 'tasa_paro_media', 'precio_vivienda']
        df_ml = df_ml.dropna(subset=features)
        X = df_ml[features]

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # K-Means con k=3
        kmeans = KMeans(n_clusters=3, random_state=42)
        df_ml['cluster'] = kmeans.fit_predict(X_scaled)

        # Asignación de nombres (Óptimo, Intermedio, Vulnerable) ordenando por la tasa de paro
        df_centroids = pd.DataFrame(scaler.inverse_transform(kmeans.cluster_centers_), columns=features)
        df_centroids['cluster_num'] = df_centroids.index
        
        # El que tenga menos paro será "Óptimo", el del medio "Intermedio", y el peor "Vulnerable"
        df_centroids = df_centroids.sort_values(by='tasa_paro_media')
        df_centroids['Cluster'] = ['Óptimo', 'Intermedio', 'Vulnerable']
        
        # Pasamos esos nombres al DataFrame principal
        mapa_nombres = dict(zip(df_centroids['cluster_num'], df_centroids['Cluster']))
        df_ml['cluster_name'] = df_ml['cluster'].map(mapa_nombres)
        
        df_centroids_mostrar = df_centroids[['Cluster', 'salario_medio', 'tasa_paro_media', 'precio_vivienda']].round(1)

    except Exception as e:
        st.error(f"Error entrenando el modelo. Revisa que las columnas 'salario_medio', 'tasa_paro_media' e 'ipv' existan en tu ML.csv. Detalle: {e}")
        return

    # --- FILA 1: Mapa y Centroides ---
    row1_col1, row1_col2 = st.columns([3, 2])
    
    with row1_col1:
        with st.container(border=True):
            st.markdown("**Mapa de clusters · CCAA 2023**")
            try:
                geojson = load_geojson()
                
                traduccion_nombres = {
                    "Madrid, Comunidad de": "Comunidad de Madrid",
                    "Navarra, Comunidad Foral de": "Comunidad Foral de Navarra",
                    "Asturias, Principado de": "Principado de Asturias",
                    "Rioja, La": "La Rioja",
                    "Murcia, Región de": "Región de Murcia",
                    "Balears, Illes": "Islas Baleares",
                    "Comunitat Valenciana": "Comunidad Valenciana",
                    "Castilla - La Mancha": "Castilla-La Mancha",
                    "Canarias": "Islas Canarias"
                }
                
                df_mapa = df_ml.copy()
                if 'comunidad' in df_mapa.columns:
                    df_mapa['comunidad'] = df_mapa['comunidad'].replace(traduccion_nombres)
                    
                    # Colores fijos para cada categoría para que siempre sea igual
                    color_map = {'Óptimo': '#2ca02c', 'Intermedio': '#ff7f0e', 'Vulnerable': '#d62728'}
                    
                    fig_map = px.choropleth(
                        df_mapa, 
                        geojson=geojson, 
                        locations='comunidad', 
                        featureidkey="properties.name", 
                        color='cluster_name',
                        color_discrete_map=color_map,
                        labels={'cluster_name': 'Grupo'}
                    )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
                    st.plotly_chart(fig_map, use_container_width=True)
                else:
                    st.warning("No se encontró la columna 'comunidad' para dibujar el mapa.")
            except Exception as e:
                st.warning(f"Error cargando el mapa: {e}")

    with row1_col2:
        with st.container(border=True):
            st.markdown("**Centroides por cluster**")
            st.dataframe(df_centroids_mostrar, use_container_width=True, hide_index=True)
            
        with st.container(border=True):
            st.markdown("**Método del codo · elección de k**")
            inertias = []
            K_range = range(1, 9)
            for k in K_range:
                km = KMeans(n_clusters=k, random_state=42).fit(X_scaled)
                inertias.append(km.inertia_)
            fig_codo = px.line(x=list(K_range), y=inertias, labels={'x': 'k', 'y': 'Inercia'})
            
            # Dibujamos un punto rojo resaltando el k=3
            fig_codo.add_scatter(x=[3], y=[inertias[2]], mode='markers', marker=dict(color='red', size=10), name='k=3')
            fig_codo.update_layout(height=250, margin=dict(t=10, b=10, l=10, r=10), showlegend=False)
            st.plotly_chart(fig_codo, use_container_width=True)

    st.write("")

    # --- FILA 2: Scatter y Tabla ---
    row2_col1, row2_col2 = st.columns(2)
    
    with row2_col1:
        with st.container(border=True):
            st.markdown("**Scatter 2D · paro vs salario coloreado por cluster**")
            color_map = {'Óptimo': '#2ca02c', 'Intermedio': '#ff7f0e', 'Vulnerable': '#d62728'}
            fig_scatter_ml = px.scatter(
                df_ml, 
                x="tasa_paro_media", 
                y="salario_medio", 
                color="cluster_name",
                hover_data=["comunidad"] if "comunidad" in df_ml.columns else [],
                color_discrete_map=color_map
            )
            st.plotly_chart(fig_scatter_ml, use_container_width=True)

    with row2_col2:
        with st.container(border=True):
            st.markdown("**CCAA por cluster · tabla detalle**")
            cols_to_show = ["comunidad", "cluster_name", "salario_medio", "tasa_paro_media"]
            cols_to_show = [c for c in cols_to_show if c in df_ml.columns]
            
            df_mostrar = df_ml[cols_to_show].sort_values(by="cluster_name")
            st.dataframe(df_mostrar, use_container_width=True, hide_index=True)