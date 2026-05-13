import streamlit as st
import pandas as pd
from utils.charts import render_html
from utils.data import load_csv, get_last_year


@st.cache_data
def get_last_unemployment_rate():
    """Extrae la tasa de paro y calcula la diferencia con el año anterior"""
    merged_df = (
        load_csv("T_empleo.csv")  # ← load_csv en vez de pd.read_csv manual
        .merge(load_csv("tbl_periodo.csv"), on="id_periodo", how="inner")
        .merge(load_csv("tbl_indicador.csv"), on="id_indicador", how="inner")
    )

    # 1. Filtramos primero por indicador, sexo y edad para limpiar la tabla
    unemployment_df = merged_df[
        merged_df["nombre"].str.contains("paro", case=False, na=False)
        & (merged_df["sexo"] == "Ambos sexos")
        & (merged_df["grupo_edad"] == "Total")
    ]

    # 2. Sacamos los años
    last_year = get_last_year(unemployment_df)
    prev_year = last_year - 1

    # 3. Calculamos los valores de ambos años
    last_val = unemployment_df[unemployment_df["anio"] == last_year]["valor"].mean()

    # Comprobamos si existe el año anterior para evitar errores
    prev_val = (
        unemployment_df[unemployment_df["anio"] == prev_year]["valor"].mean()
        if prev_year in unemployment_df["anio"].values
        else last_val
    )

    return last_val, last_val - prev_val, last_year


# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────
def show_home():

    # --- CABECERA ---
    st.title("Visión general")
    st.markdown("Evolución del poder adquisitivo de la clase trabajadora en España")
    st.info("📊 **Datos actualizados desde la API del INE - Pipeline ETL completo**")

    # --- LECTURA DE DATOS PARA KPIs ---
    try:
        # Cargamos los CSVs preprocesados
        salary_df = load_csv("Salario_Nominal_vs_Real.csv")
        housing_df = load_csv("Comparativa_Vivienda_Salario.csv")

        # KPIs Salario
        last_salary_year = int(salary_df["anio"].iloc[-1])
        prev_salary_year = int(salary_df["anio"].iloc[-2])

        # Multiplicamos por 12 para anualizar
        last_nominal_salary = salary_df["salario_nominal"].iloc[-1] * 12
        prev_nominal_salary = salary_df["salario_nominal"].iloc[-2] * 12
        salary_growth_yoy = ((last_nominal_salary / prev_nominal_salary) - 1) * 100

        last_real_salary = salary_df["salario_real"].iloc[-1] * 12

        # Pérdida de poder adquisitivo
        purchasing_power_loss = ((last_real_salary / last_nominal_salary) - 1) * 100

        # KPI Paro
        current_unemployment, unemployment_delta, unemployment_year = (
            get_last_unemployment_rate()
        )

        # KPI Vivienda
        current_hpi = housing_df["ipv"].iloc[-1]
        accumulated_housing_growth = current_hpi - 100

        current_salary_index = housing_df["indice_salario"].iloc[-1]
        accumulated_salary_growth = current_salary_index - 100

    except Exception as e:
        print(e)
        st.error(
            f"Aviso: Comprueba los nombres de las columnas de tus CSVs. Detalle técnico: {e}"
        )
        return

    # --- TARJETAS DE KPIs ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        with st.container(border=True):
            st.metric(
                label=f"Salario medio {last_salary_year}",
                value=f"{last_nominal_salary:,.0f} €".replace(",", "."),
                delta=(
                    f"+{salary_growth_yoy:.1f}% vs {prev_salary_year}"
                    if salary_growth_yoy > 0
                    else f"{salary_growth_yoy:.1f}% vs {prev_salary_year}"
                ),
            )

    with col2:
        with st.container(border=True):
            st.metric(
                label="Salario real (Deflactado)",
                value=f"{last_real_salary:,.0f} €".replace(",", "."),
                delta=f"{purchasing_power_loss:.1f}% vs Nominal",
            )

    with col3:
        with st.container(border=True):
            st.metric(
                label=f"Tasa de paro {unemployment_year}",
                value=f"{current_unemployment:.1f}%",
                delta=f"{unemployment_delta:+.1f} % vs {unemployment_year - 1}",
                delta_color="inverse",
            )
    with col4:
        with st.container(border=True):
            st.metric(
                label="IPV vs Salarios (Acum.)",
                value=f"+{accumulated_housing_growth:.0f}% / +{accumulated_salary_growth:.0f}%",
                delta="Brecha inmobiliaria",
                delta_color="inverse",
            )

    # --- GRÁFICOS ---
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        with st.container(border=True):
            st.markdown("**Salario nominal vs real · Tendencia histórica**")
            render_html("5_salario_nominal_vs_real.html")

    with chart_col2:
        with st.container(border=True):
            st.markdown("**Vivienda vs Salarios · Base 100**")
            render_html("2_vivienda_vs_salarios.html")

    # --- CONCLUSIONES ---
    with st.container(border=True):
        st.subheader("**Principales conclusiones**")

        conc_col1, conc_col2 = st.columns(2)
        with conc_col1:

            st.badge(
                "**Brecha crítica**: La vivienda crece al doble que los salarios",
                color="red",
                icon="🏠",
            )
            st.badge(
                "**Ilusión monetaria**: El salario nominal oculta la pérdida real de poder adquisitivo",
                color="yellow",
                icon="💸",
            )
        with conc_col2:
            st.badge(
                "**Mercado laboral**: Paro en reducción continua",
                color="green",
                icon="📉",
            )
            st.badge(
                "**Desigualdad territorial y de género persistentes**",
                color="blue",
                icon="🗺️",
            )
