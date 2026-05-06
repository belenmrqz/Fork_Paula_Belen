import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path


# ── FUNCIONES DE CARGA Y PROCESAMIENTO ────────────────────────────────────
@st.cache_data
def load_kpi_data():
    """Carga los datos para las métricas superiores"""
    df_salario = pd.read_csv("data_output/csv/Salario_Nominal_vs_Real.csv")
    df_vivienda = pd.read_csv("data_output/csv/Comparativa_Vivienda_Salario.csv")

    # 1. Pérdida poder adquisitivo
    salario_ultimo_nom = df_salario["salario_nominal"].iloc[-1]
    salario_ultimo_real = df_salario["salario_real"].iloc[-1]
    perdida_poder_adq = ((salario_ultimo_real / salario_ultimo_nom) - 1) * 100

    # 2. IPV Acumulado
    ipv_actual = df_vivienda["ipv"].iloc[-1]
    vivienda_acumulada = ipv_actual - 100

    return perdida_poder_adq, vivienda_acumulada


@st.cache_data
def get_salario_mediana():
    """Extrae el salario mediano del último año disponible"""
    df_desigualdad = pd.read_csv("data_output/csv/Desigualdad_Salarial.csv")

    # 1. Filtramos solo las filas que corresponden a la Mediana
    df_mediana = df_desigualdad[df_desigualdad["indicador"] == "Salario_Anual_Mediana"]

    # 2. Por seguridad, ordenamos por año y cogemos el salario de la última fila (el más reciente)
    df_mediana = df_mediana.sort_values(by="anio")
    salario_mediano_actual = df_mediana["salario"].iloc[-1]

    return salario_mediano_actual

@st.cache_data
def get_inflacion_categorias():
    """Cruza T_precios con tbl_periodo para calcular la inflación por categoría"""
    t_precios = pd.read_csv("data_output/csv/T_precios.csv")
    tbl_periodo = pd.read_csv("data_output/csv/tbl_periodo.csv")

    # JOIN para tener el año
    df_precios = t_precios.merge(tbl_periodo, on="id_periodo", how="inner")

    # Agrupamos la media anual por categoría
    df_anual = (
        df_precios.groupby(["anio", "categoria_gasto"])["valor"].mean().reset_index()
    )

    # Buscamos los dos últimos años disponibles
    ultimo_anio = df_anual["anio"].max()
    anio_anterior = ultimo_anio - 1

    # Filtramos
    df_ultimo = df_anual[df_anual["anio"] == ultimo_anio].set_index("categoria_gasto")
    df_anterior = df_anual[df_anual["anio"] == anio_anterior].set_index(
        "categoria_gasto"
    )

    # Calculamos la variación porcentual (Inflación)
    df_inflacion = ((df_ultimo["valor"] / df_anterior["valor"]) - 1) * 100
    df_inflacion = df_inflacion.reset_index().rename(columns={"valor": "inflacion_pct"})

    # Separamos el IPC General de las categorías específicas
    ipc_general = df_inflacion[df_inflacion["categoria_gasto"] == "IPC General"][
        "inflacion_pct"
    ].values
    ipc_general_val = ipc_general[0] if len(ipc_general) > 0 else 0.0

    # Dejamos solo las categorías y ordenamos de mayor a menor inflación
    df_categorias = df_inflacion[df_inflacion["categoria_gasto"] != "IPC General"]
    df_categorias = df_categorias.sort_values(by="inflacion_pct", ascending=False)

    return ipc_general_val, df_categorias, ultimo_anio


def mostrar_grafico_html(ruta_archivo):
    path = Path(ruta_archivo)
    if path.exists():
        html_content = path.read_text(encoding="utf-8")
        st.iframe(
            html_content,
            height="content",
        )
    else:
        st.warning(f"No se encontró el gráfico: {ruta_archivo}")


# ── VISTA PRINCIPAL ────────────────────────────────────────────────────────


def show_poder_adquisitivo():

    # --- CABECERA ---
    st.title("Poder Adquisitivo")
    st.markdown("Inflación, salarios reales y coste de la vivienda")

    try:
        perdida_poder_adq, vivienda_acumulada = load_kpi_data()
        ipc_general_val, df_categorias, ultimo_anio = get_inflacion_categorias()

        salario_mediana_estimado = get_salario_mediana()
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return

    # --- TARJETAS DE KPIs ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        with st.container(border=True):
            st.metric(
                label=f"IPC General {ultimo_anio}",
                value=f"{ipc_general_val:.1f}%",
                delta="Inflación interanual",
                delta_color="inverse",
                help="Índice de Precios de Consumo. Mide el encarecimiento del coste de la vida comparando la cesta de la compra de este año con la del año anterior.",
            )

    with col2:
        with st.container(border=True):
            st.metric(
                label="Pérd. poder adquisitivo",
                value=f"{perdida_poder_adq:.1f}%",
                delta="vs Salario Nominal",
                delta_color="inverse",
                help="Mide cuánto valor real ha perdido el dinero. Si los precios suben (IPC) más rápido de lo que sube el salario, el trabajador pierde capacidad de compra real.",
            )

    with col3:
        with st.container(border=True):
            st.metric(
                label="IPV acum. base 2015",
                value=f"+{vivienda_acumulada:.0f}%",
                delta="Crecimiento vivienda",
                delta_color="inverse",
                help="Índice de Precios de Vivienda. Muestra el encarecimiento acumulado desde 2015. Un +47% significa que una casa que valía 100.000€ en 2015, hoy cuesta 147.000€.",
            )

    with col4:
        with st.container(border=True):
            st.metric(
                label="Salario mediana",
                value=f"{salario_mediana_estimado:,.0f} €".replace(",", "."),
                delta="La mitad cobra menos",
                delta_color="off",
                help="Es el punto medio exacto de la población. El 50% de los trabajadores en España cobra menos de esta cantidad. Es mucho más representativo de la realidad que el 'Salario Medio', el cual se distorsiona por los sueldos extremadamente altos.",
            )

    st.write("")

    # --- GRÁFICOS FILA 1 ---
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        with st.container(border=True):
            st.markdown("**Ilusión monetaria · Nominal vs Real**")
            mostrar_grafico_html("data_output/graphics/5_salario_nominal_vs_real.html")
            st.caption(
                "💡 **Cómo interpretar:** La línea superior (Nominal) es lo que el trabajador cobra en su nómina. La línea inferior (Real) es lo que realmente puede comprar con ese dinero tras descontar la inflación. Si la línea real baja, la persona es más pobre aunque gane más dinero."
            )

    with col_g2:
        with st.container(border=True):
            st.markdown("**Inflación por categoría de gasto**")

            # --- CREACIÓN DEL GRÁFICO PLOTLY NATIVO ---
            if not df_categorias.empty:

                def acortar_nombres(nombre):
                    nombre_str = str(nombre)
                    if "Muebles" in nombre_str:
                        return "Muebles y hogar"
                    if "Vivienda, agua" in nombre_str or "agua" in nombre_str.lower():
                        return "Vivienda y suministros"
                    return nombre

                df_categorias["categoria_gasto"] = df_categorias[
                    "categoria_gasto"
                ].apply(acortar_nombres)

                fig_bar = px.bar(
                    df_categorias,
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
                st.caption(
                    "💡 **Cómo interpretar:** Muestra qué productos o servicios específicos están impulsando la inflación general. Las barras más altas e intensas indican los bienes básicos que más han asfixiado la economía de las familias en el último año."
                )
            else:
                st.info(
                    "No hay suficientes datos de categorías para calcular la inflación."
                )

    st.write("")

    # --- GRÁFICO FILA 2 (Ancho completo) ---
    with st.container(border=True):
        st.markdown("**Desigualdad salarial · Media, mediana y percentil 10**")
        mostrar_grafico_html("data_output/graphics/7_desigualdad_salarial.html")
        st.caption(
            "💡 **Cómo interpretar:** Observa la brecha entre la línea de la Media y la Mediana. Cuanto más se separan ambas líneas, mayor es la concentración de la riqueza en unos pocos. La línea P10 muestra la realidad precaria del 10% de los trabajadores que menos cobran."
        )
