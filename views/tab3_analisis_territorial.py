import streamlit as st
import pandas as pd
import plotly.express as px
from utils.charts import render_html, chart_caption
from utils.data import load_csv, load_geojson, get_last_year, rename_cols
from config.constantes import CCAA_RENAME_GEOJSON

# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────

def show_analisis_territorial():
    st.title("Análisis territorial")
    st.markdown(
        "Desglose por Comunidades Autónomas · salarios, paro y poder adquisitivo"
    )

    try:
        salaries_df = load_csv("Evolucion_Salario_Comunidades.csv")
        # 1. Obtenemos el último año dinámicamente
        last_year = get_last_year(salaries_df)

        # 2. Filtramos y ordenamos
        last_year_df = salaries_df[
            (salaries_df["anio"] == last_year)
            & (salaries_df["comunidad"] != "Total Nacional")
        ].sort_values(by="salario_medio", ascending=False)

        # 3. Calculamos métricas dinámicas para los KPIs
        top = last_year_df.iloc[0]
        bot = last_year_df.iloc[-1]
        gat_pct = ((top["salario_medio"] / bot["salario_medio"]) - 1) * 100
        ccaa_count = last_year_df["comunidad"].nunique()

    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return

    # --- TARJETAS DE KPIs ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        with st.container(border=True):
            st.metric(
                label="Salario más alto",
                value=top["comunidad"],
                delta=f"{top['salario_medio']:,.0f} €/año".replace(",", "."),
                delta_color="green",
                delta_arrow="off",
            )
    with col2:
        with st.container(border=True):
            st.metric(
                label="Salario más bajo",
                value=bot["comunidad"],
                delta=f"{bot['salario_medio']:,.0f} €/año".replace(",", "."),
                delta_color="red",
                delta_arrow="off",
            )
    with col3:
        with st.container(border=True):
            st.metric(
                label="Brecha territorial",
                value=f"+{gat_pct:.1f}%",
                delta="entre extremos",
                delta_color="red",
                delta_arrow="off",
            )
    with col4:
        with st.container(border=True):
            st.metric(
                label="CCAA analizadas",
                value=ccaa_count,
                delta=f"Dato de {last_year}",
                delta_color="off",
                delta_arrow="off",
            )

    # --- FILA 1: Mapa y Ranking ---
    row1_col1, row1_col2 = st.columns([3, 2])

    with row1_col1:
        with st.container(border=True):
            st.markdown(f"**Mapa de salarios · CCAA {last_year}**")
            try:
                geojson = load_geojson()

                map_df = last_year_df.copy()
                map_df["comunidad"] = map_df["comunidad"].replace(CCAA_RENAME_GEOJSON)

                fig_map = px.choropleth_map(
                    map_df,
                    geojson=geojson,
                    locations="comunidad",
                    featureidkey="properties.name",
                    color="salario_medio",
                    color_continuous_scale="Blues",
                    map_style="carto-positron",
                    zoom=4.7,
                    center={"lat": 40.0, "lon": -3.0},
                )
                fig_map.update_geos(fitbounds="locations", visible=False)
                fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
                st.plotly_chart(fig_map, width="stretch")
            except Exception as e:
                st.warning(f"Error cargando el mapa: {e}")

    with row1_col2:
        with st.container(border=True):
            st.markdown(f"**Ranking CCAA ({last_year})**")

            ranking_df = last_year_df[["comunidad", "salario_medio"]].copy()

            # Creamos una columna de posición numérica (Top 1, 2, 3...)
            rank_positions = range(1, len(ranking_df) + 1)
            ranking_df.insert(0, "Top", rank_positions)

            st.dataframe(
                rename_cols(ranking_df),
                width="stretch",
                hide_index=True,
            )

    # --- FILA 2: Curva de Phillips y Correlación ---
    row2_col1, row2_col2 = st.columns(2)

    with row2_col1:
        with st.container(border=True):
            st.markdown("**Curva de Phillips regional · paro vs salario**")
            render_html("4_paro_vs_salarios.html")
            chart_caption(
                "Usa el botón Play o mueve la barra de tiempo para ver la evolución. "
                "Cada burbuja es una CCAA. En los años de crisis (ej. 2012) las burbujas "
                "salen disparadas hacia la derecha."
            )

    with row2_col2:
        with st.container(border=True):
            st.markdown("**Correlación de Pearson por CCAA**")
            render_html("4b_correlacion_paro_salarios.html")
            chart_caption(
                "El coeficiente oscila entre -1 y +1. "
                "Cercano a -1 indica relación inversa fuerte: a más paro, menos salario."
            )
    # --- FILA 3: Evolución Salarial ---
    with st.container(border=True):
        st.markdown("**Evolución salarial por CCAA**")
        render_html("1_evolucion_salarios.html")
        chart_caption(
            "Haz doble clic sobre una comunidad en la leyenda para aislarla. "
            "Después, un clic normal en otras permite compararlas."
        )
