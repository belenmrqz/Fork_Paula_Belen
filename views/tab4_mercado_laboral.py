import pandas as pd
import streamlit as st
import plotly.express as px
from utils.charts import render_html, chart_caption
from utils.data import load_csv, get_last_year
from config.constantes import GENDER_COLOR_MAP

# ── CARGA ────────────────────────────────────

@st.cache_data
def load_labor_data():
    """Join de T_empleo con sus dimensiones para tener año, geografía y nombre legible."""
    return (
        load_csv("T_empleo.csv")
        .merge(load_csv("tbl_periodo.csv"), on="id_periodo", how="inner")
        .merge(load_csv("tbl_geografia.csv"), on="id_geografia", how="inner")
    )


# ── CÁLCULO DE KPIs ───────────────────────────────────────────────────────
def _kpi_contracts(quality_df):
    """Calcula el % de contratos indefinidos y su variación interanual."""
    if quality_df.empty:
        return "N/D", "Sin datos", "----"
        
    year = get_last_year(quality_df)
    actual = quality_df[quality_df["anio"] == year]["Indefinido (%)"].mean()
    prev = quality_df[quality_df["anio"] == (year - 1)]["Indefinido (%)"].mean()
    
    # Comprobación extra por si el año anterior no existe
    if pd.isna(actual) or pd.isna(prev):
        return f"{actual:.1f}%" if pd.notna(actual) else "N/D", "Sin datos previos", year
        
    return f"{actual:.1f}%", f"{actual - prev:+.1f} pp vs {year - 1}", year


def _kpi_youth_unemployment(employment_df):
    """Calcula la tasa de paro juvenil comparándola con el segmento senior (>55 años)."""
    
    base = employment_df[
        (employment_df["nombre"] == "Total Nacional") & 
        (employment_df["sexo"] == "Ambos sexos")
    ]
    
    if base.empty:
        return "N/D", "Filtro base vacío", "----"
        
    year = get_last_year(base)
    
    youth = base[(base["grupo_edad"] == "Menores de 25 años") & (base["anio"] == year)]["valor"].mean()
    elders = base[(base["grupo_edad"] == "De 55 y más años") & (base["anio"] == year)]["valor"].mean()
    
    if pd.isna(youth) or pd.isna(elders):
        return "N/D", "Falta edad", year
        
    return f"{youth:.1f}%", f"{youth - elders:+.1f} pp vs >55 años", year


def _kpi_gap(gap_df):
    """Calcula la brecha salarial porcentual del último año y su evolución."""
    if gap_df.empty:
        return "N/D", "Sin datos", "----"
        
    year = get_last_year(gap_df)
    actual = gap_df[gap_df["anio"] == year]["brecha_porcentual"].mean()
    previous = gap_df[gap_df["anio"] == (year - 1)]["brecha_porcentual"].mean()
    
    if pd.isna(actual) or pd.isna(previous):
        return f"{actual:.1f}%" if pd.notna(actual) else "N/D", "Sin datos previos", year
        
    return f"{actual:.1f}%", f"{actual - previous:+.1f} pp vs {year - 1}", year


def _kpi_national_unemployment(employment_df):
    """Calcula la tasa de paro media nacional."""
    base = employment_df[
        (employment_df["nombre"] == "Total Nacional") & 
        (employment_df["sexo"] == "Ambos sexos") &
        (employment_df["grupo_edad"] == "Total")

    ]
    
    if base.empty:
        return "N/D", "Filtro base vacío", "----"
        
    year = get_last_year(base)
        
    actual = base[base["anio"] == year]["valor"].mean()
    previous = base[base["anio"] == (year - 1)]["valor"].mean()
    
    if pd.isna(actual) or pd.isna(previous):
        return f"{actual:.1f}%" if pd.notna(actual) else "N/D", "Sin datos previos", year
        
    return f"{actual:.1f}%", f"{actual - previous:+.1f} pp vs {year - 1}", year


# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────

def show_mercado_laboral():
    st.title("Mercado laboral")
    st.markdown("Empleo, temporalidad y brecha salarial de género")

    try:
        # Carga de datasets procesados
        employment_df = load_labor_data()
        quality_df = load_csv("Calidad_Empleo.csv")
        gap_df = load_csv("Brecha_Salarial_Ocupacion.csv")

        # Invocación de funciones de cálculo para KPIs
        val_indef, indef_delta, year_indef = _kpi_contracts(quality_df)
        val_juv, juv_delta, year_juv = _kpi_youth_unemployment(employment_df)
        val_gap, gap_delta, year_gap = _kpi_gap(gap_df)
        val_unempl, unempl_delta, year_unemployment = _kpi_national_unemployment(employment_df)

    except Exception as e:
        st.warning(f"Error vinculando KPIs: {e}")
        val_indef = val_juv = val_gap = val_unempl = "---"
        indef_delta = juv_delta = gap_delta = unempl_delta = None
        year_indef = year_juv = year_gap = year_unemployment = ""

    # --- TARJETAS DE KPIs ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        with st.container(border=True):
            st.metric(f"Contratos indefinidos {year_indef}", val_indef, indef_delta)
    with col2:
        with st.container(border=True):
            st.metric(
                f"Paro juvenil (<25 años) {year_juv}",
                val_juv,
                juv_delta,
                delta_color="inverse",
            )
    with col3:
        with st.container(border=True):
            st.metric(
                f"Brecha de género {year_gap}",
                val_gap,
                gap_delta,
                delta_color="inverse",
            )
    with col4:
        with st.container(border=True):
            st.metric(
                f"Paro nacional {year_unemployment}",
                val_unempl,
                unempl_delta,
                delta_color="inverse",
            )

    # --- FILA 1: Calidad Empleo y Brecha ---
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        with st.container(border=True):
            st.markdown("**Calidad del empleo**")
            render_html("6_calidad_empleo.html")
            chart_caption(
                "**Cómo interpretar:** El gráfico de área muestra el peso de cada tipo de contrato sobre el total. Fíjate en cómo a partir de 2022 el área verde (indefinidos) se ensancha abruptamente, reduciendo drásticamente la temporalidad gracias a la reforma laboral."
            )

    with row1_col2:
        with st.container(border=True):
            st.markdown("**Brecha salarial por ocupación**")
            render_html("3_brecha_salarial.html")
            chart_caption(
                "**Cómo interpretar:** Muestra la diferencia salarial porcentual entre hombres y mujeres por ocupación. A mayor longitud y oscuridad de la barra, mayor es la desigualdad de género en ese sector específico."
            )

    # --- FILA 2: Paro Edad/Sexo y Evolución ---
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        with st.container(border=True):
            st.markdown("**Paro por edad y sexo**")

            try:
                # Selectores de filtrado para el gráfico interactivo               
                col_filt1, col_filt2 = st.columns(2)
                with col_filt1:
                    available_years = employment_df["anio"].unique()
                    sel_year = st.selectbox("Año:", sorted(available_years, reverse=True))
                with col_filt2:
                    # Obtenemos lista de regiones excluyendo el total nacional para el orden alfabético
                    ccaa_list = [
                        c
                        for c in employment_df["nombre"].dropna().unique()
                        if c != "Total Nacional"
                    ]
                    available_ccaa = ["Total Nacional"] + sorted(ccaa_list)
                    sel_ccaa = st.selectbox("Comunidad:", available_ccaa)

                # Obtenemos lista de regiones excluyendo el total nacional para el orden alfabético
                EXCLUDE_AGES = [
                    "Total",
                    "Menores de 25 años",
                    "25 y más años",
                ]
                AGE_ORDER = [
                    "De 16 a 19 años",
                    "De 20 a 24 años",
                    "De 25 a 54 años",
                    "De 55 y más años",
                ]

                # Procesamiento de datos filtrados para el gráfico de barras
                filtered_labol_df = (
                    employment_df[
                        (employment_df["anio"] == sel_year)
                        & (employment_df["nombre"] == sel_ccaa)
                        & (~employment_df["sexo"].isin(["Ambos sexos", "Total"]))
                        & (~employment_df["grupo_edad"].isin(EXCLUDE_AGES))
                    ]
                    .groupby(["grupo_edad", "sexo"])["valor"]
                    .mean()
                    .reset_index()
                )

                # Generación del gráfico de barras agrupadas por sexo
                fig_age = px.bar(
                    filtered_labol_df,
                    x="grupo_edad",
                    y="valor",
                    color="sexo",
                    barmode="group",
                    color_discrete_map=GENDER_COLOR_MAP,
                    labels={
                        "valor": "Tasa de paro (%)",
                        "grupo_edad": "Rango de edad",
                        "sexo": "Sexo",
                    },
                    category_orders={"grupo_edad": AGE_ORDER},
                )

                fig_age.update_layout(
                    margin=dict(t=20, b=10, l=10, r=10),
                    legend=dict(
                        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                    ),
                )

                st.plotly_chart(fig_age, width="stretch")
                chart_caption(
                    "**Cómo interpretar:** Compara la tasa de desempleo entre hombres y mujeres por tramos de edad. Facilita la detección de brechas de género específicas y permite visualizar el impacto del paro en las distintas etapas de la vida laboral."
                )

            except Exception as e:
                st.error(f"Revisa el filtrado de T_empleo.csv: {e}")

    with row2_col2:
        with st.container(border=True):
            st.markdown("**Evolución tasa de paro · Vista facetada**")
            render_html("8_paro_facetado.html")
            chart_caption(
                "**Cómo interpretar:** Cada minigráfico muestra la evolución del paro en una CCAA. Permite comparar visualmente qué regiones sufren mayores subidas durante las crisis (picos) y cuáles mantienen una mayor estabilidad laboral."
            )
