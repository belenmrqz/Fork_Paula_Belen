import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import streamlit.components.v1 as components

# ── FUNCIONES DE CARGA Y PROCESAMIENTO ────────────────────────────────────

def mostrar_grafico_html(ruta_archivo):
    """Lee y muestra un archivo HTML exportado previamente por Plotly"""
    path = Path(ruta_archivo)
    if path.exists():
        html_content = path.read_text(encoding="utf-8")
        components.html(
            html_content,
            height=500,
        )
    else:
        st.warning(f"No se encontró el gráfico: {ruta_archivo}")

@st.cache_data
def load_labor_data():
    df_empleo_raw = pd.read_csv("data_output/csv/T_empleo.csv")
    tbl_periodo = pd.read_csv("data_output/csv/tbl_periodo.csv")
    tbl_geografia = pd.read_csv("data_output/csv/tbl_geografia.csv")
    
    df_empleo = df_empleo_raw.merge(tbl_periodo, on="id_periodo", how="inner")
    df_empleo = df_empleo.merge(tbl_geografia, on="id_geografia", how="inner")
    return df_empleo

# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────

def show_mercado_laboral():
    st.title("Mercado laboral")
    st.markdown("Empleo, temporalidad y brecha salarial de género")

    try:
        df_empleo = load_labor_data()
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return

    # --- TARJETAS DE KPIs ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        with st.container(border=True):
            st.metric("Contratos indefinidos 2023", "84.7%", "+12pp desde 2020")
    with col2:
        with st.container(border=True):
            st.metric("Paro juvenil 16-19", "53.4%", "- Máximo vulnerable")
    with col3:
        with st.container(border=True):
            st.metric("Brecha salarial media", "~16%", "- Todas ocupaciones")
    with col4:
        with st.container(border=True):
            st.metric("Tasa paro nacional", "11.8%", "Mínimo histórico")

    st.write("")

    # --- FILA 1: Calidad Empleo y Brecha ---
    row1_col1, row1_col2 = st.columns(2)
    
    with row1_col1:
        with st.container(border=True):
            st.markdown("**Calidad del empleo · Contratos**")
            mostrar_grafico_html("data_output/graphics/6_calidad_empleo.html")
            st.caption("💡 **Cómo interpretar:** Visualiza el efecto cruzado de la reforma laboral, donde los indefinidos superan históricamente a los temporales.")

    with row1_col2:
        with st.container(border=True):
            st.markdown("**Brecha salarial por ocupación**")
            mostrar_grafico_html("data_output/graphics/3_brecha_salarial.html")

    st.write("")

    # --- FILA 2: Paro Edad/Sexo y Evolución ---
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        with st.container(border=True):
            st.markdown("**Paro por edad y sexo**")
            
            try:
                col_filt1, col_filt2 = st.columns(2)
                with col_filt1:
                    anios_disp = df_empleo['anio'].unique()
                    anio_sel = st.selectbox("Año:", sorted(anios_disp, reverse=True))
                with col_filt2:
                    ccaa_disp = ["Total Nacional"] + list(df_empleo['nombre'].dropna().unique())
                    ccaa_sel = st.selectbox("Comunidad:", ccaa_disp)
                
                df_filt = df_empleo[(df_empleo['anio'] == anio_sel)]
                if ccaa_sel != "Total Nacional":
                    df_filt = df_filt[df_filt['nombre'] == ccaa_sel]

                fig_edad = px.bar(
                    df_filt, 
                    x="grupo_edad", 
                    y="valor", 
                    color="sexo", 
                    barmode="group",
                    labels={"valor": "Tasa de paro (%)", "grupo_edad": "Rango de edad"}
                )
                st.plotly_chart(fig_edad, use_container_width=True)
            except Exception as e:
                st.error(f"Revisa nombres columnas T_empleo.csv: {e}")

    with row2_col2:
        with st.container(border=True):
            st.markdown("**Evolución tasa de paro · Vista facetada**")
            mostrar_grafico_html("data_output/graphics/8_paro_facetado.html")