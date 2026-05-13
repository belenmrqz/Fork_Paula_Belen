import pandas as pd
import streamlit as st
import plotly.express as px
from config.constantes import CLUSTER_COLOR_MAP, CLUSTER_ORDER
from utils.data import load_csv, load_geojson, get_last_year, rename_cols
from utils.charts import render_html, chart_caption

# ── VISTA PRINCIPAL DEL CLUSTERING ─────────────────────────────────────────
def show_clustering():
    """
    Renderiza el análisis de segmentación regional mediante K-Means.
    Permite visualizar grupos de CCAA con realidades socioeconómicas similares.
    """
    try:
        df_ml = load_csv("ML_clustering.csv")
        geojson = load_geojson()

        # Convertimos la columna a categórica para asegurar que el orden de los clusters
        # en la leyenda sea siempre: Óptimo, Intermedio y Vulnerable.
        df_ml['Estado Financiero'] = pd.Categorical(
            df_ml['Estado Financiero'], 
            categories=CLUSTER_ORDER, 
            ordered=True
        )

        last_year = get_last_year(df_ml)

    except Exception as e:
        st.error(f"Error cargando los datos de ML: {e}")
        return
    st.title("Clustering · Análisis Regional")

    # --- FILA 1: Mapa y Centroides ---
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        with st.container(border=True):
            st.markdown(f"**Mapa de clusters  {last_year}**")

            # Generación del mapa temático basado en los grupos del algoritmo
            fig_mapa = px.choropleth_map(
                df_ml,
                geojson=geojson,
                featureidkey="properties.name",
                locations="Nombre_Mapa",
                color="Estado Financiero",
                color_discrete_map=CLUSTER_COLOR_MAP,
                hover_name="comunidad",
                hover_data={
                    "Nombre_Mapa": False,
                    "Estado Financiero": False,
                    "Salario": True,
                    "Paro": True,
                },
                map_style="carto-positron",
                zoom=4.7,
                center={"lat": 40.0, "lon": -3.0},
            )
            fig_mapa.update_layout(
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    title=None,
                ),
            )

            st.plotly_chart(
                fig_mapa,
                width="stretch",
            )
            chart_caption(
                "**Cómo interpretar:** Comunidades agrupadas por el algoritmo K-Means. El verde indica el equilibrio ideal entre sueldos altos y desempleo contenido."
            )

    with row1_col2:
        with st.container(border=True):
            st.markdown("**Centroides por cluster**")
            # Calculamos la media de cada variable por cluster para entender qué define a cada grupo
            centroids_df = (
                df_ml.groupby("Estado Financiero", observed=False)[
                    ["salario_medio", "tasa_paro_media", "precio_vivienda"]
                ]
                .mean()
                .reset_index()
            )
            # Mostramos los promedios del grupo con nombres de columna legibles
            st.dataframe(rename_cols(centroids_df), width="stretch", hide_index=True)
            chart_caption(
                "Grupo óptimo: mejor salario pero mayor presión inmobiliaria."
            )
        with st.container(border=True):
            st.markdown("**Validación: Método del Codo**")            
            render_html("clustering_graphics/codo_kmeans.html", height=190)
            chart_caption(
                "Elegimos k=3 porque es el punto donde la curva hace 'codo', optimizando el error de agrupación."
            )

    # --- FILA 2: Scatter y Tabla ---
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        with st.container(border=True):
            st.markdown("**Relación Salario vs Paro**")

            # Scatter plot para visualizar la separación física de los clusters
            fig_sc = px.scatter(
                df_ml,
                x="tasa_paro_media",
                y="salario_medio",
                color="Estado Financiero",
                hover_name="comunidad",
                color_discrete_map=CLUSTER_COLOR_MAP,
                labels={"tasa_paro_media": "Paro (%)", "salario_medio": "Salario (€)"},
            )

            fig_sc.update_layout(
                margin=dict(t=10, b=10, l=10, r=10),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    title=None,
                ),
            )
            st.plotly_chart(fig_sc, width="stretch", height=350)
            chart_caption(
                "**Análisis:** Identifica visualmente qué regiones lideran la economía y cuáles presentan mayor vulnerabilidad laboral."
            )

    with row2_col2:
        with st.container(border=True):
            st.markdown("**Clasificación por CCAA**")

            df_list = df_ml[
                ["comunidad", "Estado Financiero", "salario_medio", "tasa_paro_media"]
            ].sort_values(by="Estado Financiero")

            # Tabla final con la asignación individual de cada comunidad autónoma
            st.dataframe(
                rename_cols(df_list),
                width="stretch",
                hide_index=True,
            )
