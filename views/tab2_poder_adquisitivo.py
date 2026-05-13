import streamlit as st
import plotly.express as px
from utils.charts import render_html, chart_caption
from utils.data import load_csv, get_last_year


# ── FUNCIONES DE CARGA Y PROCESAMIENTO ────────────────────────────────────
@st.cache_data
def load_kpi_data():
    """Carga los datos para las métricas superiores"""
    salary_df = load_csv("Salario_Nominal_vs_Real.csv")
    housing_df = load_csv("Comparativa_Vivienda_Salario.csv")

    # 1. Pérdida poder adquisitivo: Calculamos la caída porcentual entre el real y el nominal
    purchasing_power_loss = (
        (salary_df["salario_real"].iloc[-1] / salary_df["salario_nominal"].iloc[-1])
        - 1
    ) * 100

    # 2. IPV Acumulado: Restamos la base 100 para obtener el porcentaje de crecimiento neto
    accumulated_housing = housing_df["ipv"].iloc[-1] - 100

    return purchasing_power_loss, accumulated_housing


@st.cache_data
def get_median_salary():
    """Extrae el salario mediano del último año disponible"""
    df = load_csv("Desigualdad_Salarial.csv")
    return (
        df[df["indicador"] == "Salario_Anual_Mediana"]
        .sort_values("anio")["salario"]
        .iloc[-1]
    )


@st.cache_data
def get_category_inflation():
    """Cruza T_precios con tbl_periodo para calcular la inflación por categoría"""
    prices_df = load_csv("T_precios.csv").merge(
        load_csv("tbl_periodo.csv"), on="id_periodo", how="inner"
    )

    # Agrupamos la media anual por categoría
    annual_df = (
        prices_df.groupby(["anio", "categoria_gasto"])["valor"].mean().reset_index()
    )

    # Buscamos los dos últimos años disponibles
    last_year = get_last_year(annual_df)
    previous_year = last_year - 1

    # Filtramos
    last_year_df = annual_df[annual_df["anio"] == last_year].set_index("categoria_gasto")
    previous_year_df = annual_df[annual_df["anio"] == previous_year].set_index(
        "categoria_gasto"
    )

    # Calculamos la variación porcentual (Inflación)
    inflation_df = (
        (((last_year_df["valor"] / previous_year_df["valor"]) - 1) * 100)
        .reset_index()
        .rename(columns={"valor": "inflacion_pct"})
    )

    # Separamos el IPC General de las categorías específicas
    general_cpi = inflation_df[inflation_df["categoria_gasto"] == "IPC General"][
        "inflacion_pct"
    ].values
    general_cpi_val = general_cpi[0] if len(general_cpi) > 0 else 0.0

    # Dejamos solo las categorías y ordenamos de mayor a menor inflación
    categories_df = inflation_df[
        inflation_df["categoria_gasto"] != "IPC General"
    ].sort_values("inflacion_pct", ascending=False)

    return general_cpi_val, categories_df, last_year


# Nombres largos del INE → etiquetas cortas para el gráfico
_SHORT_NAMES = {
    "Muebles": "Muebles y hogar",
    "Vivienda, agua": "Vivienda y suministros",
    "agua": "Vivienda y suministros",
}


def _shorten_category(name: str) -> str:
    """Helper para acortar las categorías de gasto del INE en los ejes del gráfico"""
    for clave, replacement in _SHORT_NAMES.items():
        if clave.lower() in name.lower():
            return replacement
    return name


# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────


def show_purchasing_power():

    # --- CABECERA ---
    st.title("Poder Adquisitivo")
    st.markdown("Inflación, salarios reales y coste de la vivienda")

    try:
        purchasing_power_loss, accumulated_housing = load_kpi_data()
        ipc_general_val, categories_df, last_year = get_category_inflation()
        estimated_median_salary = get_median_salary()
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return

    # --- TARJETAS DE KPIs ---
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)

    with kpi_col1:
        with st.container(border=True):
            st.metric(
                label=f"IPC General {last_year}",
                value=f"{ipc_general_val:.1f}%",
                delta="Inflación interanual",
                delta_color="inverse",
                help="Índice de Precios de Consumo. Mide el encarecimiento del coste de la vida comparando la cesta de la compra de este año con la del año anterior.",
            )

    with kpi_col2:
        with st.container(border=True):
            st.metric(
                label="Pérd. poder adquisitivo",
                value=f"{purchasing_power_loss:.1f}%",
                delta="vs Salario Nominal",
                delta_color="inverse",
                help="Mide cuánto valor real ha perdido el dinero. Si los precios suben (IPC) más rápido de lo que sube el salario, el trabajador pierde capacidad de compra real.",
            )

    with kpi_col3:
        with st.container(border=True):
            st.metric(
                label="IPV acum. base 2015",
                value=f"+{accumulated_housing:.0f}%",
                delta="Crecimiento vivienda",
                delta_color="inverse",
                help="Índice de Precios de Vivienda. Muestra el encarecimiento acumulado desde 2015. Un +47% significa que una casa que valía 100.000€ en 2015, hoy cuesta 147.000€.",
            )

    with kpi_col4:
        with st.container(border=True):
            st.metric(
                label="Salario mediana",
                value=f"{estimated_median_salary:,.0f} €".replace(",", "."),
                delta="La mitad cobra menos",
                delta_color="off",
                help="Es el punto medio exacto de la población. El 50% de los trabajadores en España cobra menos de esta cantidad. Es mucho más representativo de la realidad que el 'Salario Medio', el cual se distorsiona por los sueldos extremadamente altos.",
            )

    st.write("")

    # --- GRÁFICOS FILA 1 ---
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        with st.container(border=True):
            st.markdown("**Ilusión monetaria · Nominal vs Real**")
            render_html("5_salario_nominal_vs_real.html")
            chart_caption(
                "La línea Nominal es lo que el trabajador cobra en nómina. "
                "La línea Real es lo que puede comprar con ese dinero tras descontar la inflación. "
                "Si la línea real baja, la persona es más pobre aunque gane más."
            )

    with chart_col2:
        with st.container(border=True):
            st.markdown("**Inflación por categoría de gasto**")

            if not categories_df.empty:

                categories_df["categoria_gasto"] = categories_df[
                    "categoria_gasto"
                ].apply(_shorten_category)

                fig_bar = px.bar(
                    categories_df,
                    x="categoria_gasto",
                    y="inflacion_pct",
                    color="inflacion_pct",
                    color_continuous_scale="Reds",  # Escala de rojos
                    text_auto=".1f",  # Muestra el número encima de la barra
                )

                # Diseño limpio
                fig_bar.update_layout(
                    xaxis_title=None,
                    yaxis_title="% Inflación",
                    coloraxis_showscale=False,  # Oculta la barra de leyenda de color para ahorrar espacio
                    margin=dict(t=10, b=10, l=10, r=10),
                )
                # Mejoramos la rotación del texto en el eje X si hay nombres muy largos
                fig_bar.update_xaxes(tickangle=45)

                st.plotly_chart(fig_bar, width="content")
                chart_caption(
                    "Las barras más altas e intensas indican los bienes que más han "
                    "encarecido la vida de las familias en el último año."
                )
            else:
                st.info(
                    "No hay suficientes datos de categorías para calcular la inflación."
                )

    st.write("")

    # --- GRÁFICO FILA 2 (Ancho completo) ---
    with st.container(border=True):
        st.markdown("**Desigualdad salarial · Media, mediana y percentil 10**")
        render_html("7_desigualdad_salarial.html")
        chart_caption(
            "Cuanto más se separan la Media y la Mediana, mayor es la concentración "
            "de riqueza. La línea P10 muestra la realidad del 10% que menos cobra."
        )
