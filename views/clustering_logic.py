import streamlit as st
import pandas as pd
from pathlib import Path
import streamlit.components.v1 as components
import plotly.express as px
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# ── FUNCIONES DE AYUDA Y CARGA ─────────────────────────────────────────────
def mostrar_grafico_html(ruta_archivo, height=500):
    """Lee y muestra un archivo HTML exportado previamente por Plotly"""
    path = Path(ruta_archivo)
    if path.exists():
        html_content = path.read_text(encoding="utf-8")
        components.html(
            html_content,
            height=height,
            scrolling=False
        )
    else:
        st.warning(f"No se encontró el gráfico: {ruta_archivo}")

@st.cache_data
def load_ml_data():
    return pd.read_csv("data_output/csv/ML.csv")

# ── VISTA PRINCIPAL DEL CLUSTERING ─────────────────────────────────────────
def show_clustering():
    try:
        df_ml_raw = load_ml_data()
        ultimo_anio = df_ml_raw['anio'].max()
        df_ml = df_ml_raw[df_ml_raw['anio'] == ultimo_anio].copy()
    except Exception as e:
        st.error(f"Error cargando los datos de ML: {e}")
        return

    # --- ENTRENAMIENTO PARA LAS TABLAS Y GRÁFICOS NATIVOS ---
    try:
        features = ['salario_medio', 'tasa_paro_media', 'precio_vivienda']
        df_ml = df_ml.dropna(subset=features)
        X = df_ml[features]

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # K-Means con k=3
        kmeans = KMeans(n_clusters=3, random_state=42)
        df_ml['cluster'] = kmeans.fit_predict(X_scaled)

        # Calculamos centroides para la tabla
        df_centroids = pd.DataFrame(scaler.inverse_transform(kmeans.cluster_centers_), columns=features)
        df_centroids['cluster_num'] = df_centroids.index
        
        # Asignamos nombres por lógica
        df_centroids = df_centroids.sort_values(by='tasa_paro_media')
        df_centroids['Cluster'] = ['Óptimo', 'Intermedio', 'Vulnerable']
        
        mapa_nombres = dict(zip(df_centroids['cluster_num'], df_centroids['Cluster']))
        df_ml['cluster_name'] = df_ml['cluster'].map(mapa_nombres)
        
        df_centroids_mostrar = df_centroids[['Cluster', 'salario_medio', 'tasa_paro_media', 'precio_vivienda']].round(1)

    except Exception as e:
        st.error(f"Error entrenando el modelo. Detalle: {e}")
        return

    # --- FILA 1: Mapa y Centroides ---
    row1_col1, row1_col2 = st.columns([3, 2])
    
    with row1_col1:
        with st.container(border=True):
            st.markdown(f"**Mapa de clusters · CCAA {ultimo_anio}**")
            mostrar_grafico_html("data_output/graphics/clustering_graphics/mapa_interactivo_por_ccaa.html", height=500)

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
            if "comunidad" in df_ml.columns:
                cols_to_show = ["comunidad", "cluster_name", "salario_medio", "tasa_paro_media"]
                df_mostrar = df_ml[cols_to_show].sort_values(by="cluster_name")
                st.dataframe(df_mostrar, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_ml, use_container_width=True)