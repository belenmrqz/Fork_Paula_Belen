import streamlit as st
import pandas as pd
import plotly.express as px

# ── FUNCIONES DE CARGA ─────────────────────────────────────────────────────
@st.cache_data
def load_labor_data():
    df_job_quality = pd.read_csv("data_output/csv/Calidad_Empleo.csv")
    df_gap = pd.read_csv("data_output/csv/Brecha_Salarial_Ocupacion.csv")
    df_paro_hist = pd.read_csv("data_output/csv/Relacion_Paro_Salarios.csv") # Usamos el mismo del territorial para la evolución
    
    # ── CRUCE DE TABLAS PARA T_EMPLEO ──
    df_empleo_raw = pd.read_csv("data_output/csv/T_empleo.csv")
    tbl_periodo = pd.read_csv("data_output/csv/tbl_periodo.csv")
    tbl_geografia = pd.read_csv("data_output/csv/tbl_geografia.csv")
    
    # Unimos con periodo para tener la columna 'anio'
    df_empleo = df_empleo_raw.merge(tbl_periodo, on="id_periodo", how="inner")
    
    # Unimos con geografia para tener el nombre de la comunidad
    df_empleo = df_empleo.merge(tbl_geografia, on="id_geografia", how="inner")
    
    return df_job_quality, df_gap, df_empleo, df_paro_hist

# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────
def show_mercado_laboral():
    st.title("Mercado laboral")
    st.markdown("Empleo, temporalidad y brecha salarial de género")

    try:
        df_job_quality, df_gap, df_empleo, df_paro_hist = load_labor_data()
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
            try:
                fig_quality = px.line(
                                    df_job_quality, 
                                    x="anio", 
                                    y=["Indefinido (%)", "Temporal (%)"],
                                    labels={"value": "Porcentaje (%)", "variable": "Tipo de contrato"}
                                )                
                fig_quality.add_vline(x=2022, line_dash="dash", line_color="green", annotation_text="Reforma 2022")
                st.plotly_chart(fig_quality, use_container_width=True)
                st.caption("💡 **Cómo interpretar:** Visualiza el efecto cruzado de la reforma laboral, donde los indefinidos superan históricamente a los temporales.")
            except Exception as e:
                st.error(f"Revisa nombres columnas Calidad_Empleo.csv: {e}")

    with row1_col2:
        with st.container(border=True):
            st.markdown("**Brecha salarial por ocupación**")
            try:
                fig_gap = px.bar(
                    df_gap, 
                    x="brecha_porcentual",
                    y="ocupacion",
                    orientation='h', 
                    color="brecha_porcentual", 
                    color_continuous_scale="RdYlBu_r"
                )
                fig_gap.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_gap, use_container_width=True)
            except Exception as e:
                 st.error(f"Revisa nombres columnas Brecha_Salarial_Ocupacion.csv: {e}")

    st.write("")

    # --- FILA 2: Paro Edad/Sexo y Evolución ---
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        with st.container(border=True):
            st.markdown("**Paro por edad y sexo**")
            
            try:
                col_filt1, col_filt2 = st.columns(2)
                columna_geo = "nombre"
                with col_filt1:
                    anios_disp = df_empleo['anio'].unique()
                    anio_sel = st.selectbox("Año:", sorted(anios_disp, reverse=True))
                with col_filt2:
                    ccaa_disp = ["Total Nacional"] + list(df_empleo['nombre'].dropna().unique())
                    ccaa_sel = st.selectbox("Comunidad:", ccaa_disp)
                
                df_filt = df_empleo[(df_empleo['anio'] == anio_sel)]
                if ccaa_sel != "Total Nacional":
                    df_filt = df_filt[df_filt['nombre'] == ccaa_sel]

                # Gráfico con tus columnas de T_empleo
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
            try:
                fig_facet = px.line(
                    df_paro_hist, 
                    x="anio", 
                    y="tasa_paro_media", 
                    facet_col="comunidad", 
                    facet_col_wrap=4
                )
                fig_facet.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10))
                fig_facet.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
                st.plotly_chart(fig_facet, use_container_width=True)
            except Exception as e:
                st.error(f"Revisa nombres columnas para vista facetada: {e}")