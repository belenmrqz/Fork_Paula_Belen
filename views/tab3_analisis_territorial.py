import streamlit as st
import pandas as pd
import plotly.express as px
import json
import requests

# ── FUNCIONES DE CARGA Y PROCESAMIENTO ────────────────────────────────────
@st.cache_data
def load_territorial_data():
    df_salarios = pd.read_csv("data_output/csv/Evolucion_Salario_Comunidades.csv")
    df_paro_salarios = pd.read_csv("data_output/csv/Relacion_Paro_Salarios.csv")
    df_corr = pd.read_csv("data_output/csv/Correlacion_Paro_Salarios.csv")
    return df_salarios, df_paro_salarios, df_corr

@st.cache_data
def load_geojson():
    url = "https://raw.githubusercontent.com/R-CoderDotCom/data/main/shapefile_spain/spain.geojson"
    respuesta = requests.get(url)
    return respuesta.json()

# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────
def show_analisis_territorial():
    st.title("Análisis territorial")
    st.markdown("Desglose por Comunidades Autónomas · salarios, paro y poder adquisitivo")

    try:
        df_salarios, df_paro_salarios, df_corr = load_territorial_data()
        nombre_col_salario = "salario_medio" 
        df_2023 = df_salarios[df_salarios["anio"] == 2023].sort_values(by=nombre_col_salario, ascending=False)
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return

    # --- TARJETAS DE KPIs ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        with st.container(border=True):
            st.metric(label="CCAA con salario más alto", value="País Vasco", delta="32.100 €/año", delta_color="off")
    with col2:
        with st.container(border=True):
            st.metric(label="CCAA con salario más bajo", value="Extremadura", delta="21.400 €/año", delta_color="off")
    with col3:
        with st.container(border=True):
            st.metric(label="Brecha territorial", value="+50%", delta="entre extremos", delta_color="off")
    with col4:
        with st.container(border=True):
            st.metric(label="CCAA analizadas", value="19", delta="datos INE 2008-2023", delta_color="off")

    st.write("")

    # --- FILA 1: Mapa y Ranking ---
    row1_col1, row1_col2 = st.columns([3, 2])
    
    with row1_col1:
        with st.container(border=True):
            st.markdown("**Mapa de salarios · CCAA**")
            try:
                geojson = load_geojson()
                
                # 1. Creamos el diccionario traductor
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
                
                # 2. Hacemos una copia de los datos solo para el mapa y le aplicamos el traductor
                df_mapa = df_2023.copy()
                df_mapa["comunidad"] = df_mapa["comunidad"].replace(traduccion_nombres)
                
                # 3. Dibujamos el mapa con df_mapa en lugar de df_2023
                fig_map = px.choropleth(
                    df_mapa, 
                    geojson=geojson, 
                    locations='comunidad', 
                    featureidkey="properties.name", 
                    color=nombre_col_salario, 
                    color_continuous_scale="Blues"
                )
                fig_map.update_geos(fitbounds="locations", visible=False)
                fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
                st.plotly_chart(fig_map, use_container_width=True)
            except Exception as e:
                st.warning(f"Error cargando el mapa: {e}")
                
    with row1_col2:
        with st.container(border=True):
            st.markdown("**Ranking CCAA (2023)**")
            st.dataframe(
                df_2023[["comunidad", nombre_col_salario]], 
                use_container_width=True, 
                hide_index=True
            )

    st.write("")

    # --- FILA 2: Curva de Phillips y Correlación ---
    row2_col1, row2_col2 = st.columns(2)
    
    with row2_col1:
        with st.container(border=True):
            st.markdown("**Curva de Phillips regional · paro vs salario**")
            fig_scatter = px.scatter(
                df_paro_salarios, 
                x="tasa_paro_media", 
                y=nombre_col_salario,
                color="comunidad", 
                trendline="ols"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
            st.caption("💡 **Cómo interpretar:** Muestra si existe una relación entre un mayor nivel de paro y salarios más bajos en las distintas regiones.")

    with row2_col2:
        with st.container(border=True):
            st.markdown("**Correlación de Pearson por CCAA**")
            fig_corr = px.bar(
                df_corr, 
                x="correlacion_pearson",
                y="comunidad",
                orientation='h', 
                color="correlacion_pearson", 
                color_continuous_scale="RdBu"
            )
            fig_corr.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_corr, use_container_width=True)

    st.write("")

    # --- FILA 3: Evolución Salarial ---
    with st.container(border=True):
        st.markdown("**Evolución salarial por CCAA**")
        ccaa_seleccionadas = st.multiselect(
            "Selecciona regiones para comparar", 
            options=df_salarios['comunidad'].unique(),
            default=["País Vasco", "Madrid, Comunidad de", "Extremadura"]
        )
        
        if ccaa_seleccionadas:
            df_filtrado = df_salarios[df_salarios['comunidad'].isin(ccaa_seleccionadas)]
            fig_line = px.line(df_filtrado, x="anio", y=nombre_col_salario, color="comunidad")
            st.plotly_chart(fig_line, use_container_width=True)