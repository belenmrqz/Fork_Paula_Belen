import json
import streamlit as st
import joblib
import plotly.express as px
import pandas as pd
from pathlib import Path

# ── HELPERS / FUNCIONES AUXILIARES ─────────────────────────────────────────
# Usamos @st.cache_data y @st.cache_resource para guardar en la memoria RAM
# de la aplicación estos datos y no tener que recargarlos cada vez que el
# usuario toca un botón.


@st.cache_data
def load_historic_data() -> pd.DataFrame:
    """Carga el dataset histórico limpio y listo para analítica."""
    return pd.read_csv("data_output/csv/ML.csv")


@st.cache_resource
def load_model():
    """
    Carga los artefactos del modelo de Machine Learning desde la carpeta local.
    Retorna el modelo, el codificador de variables categóricas (target_encoder)
    y el escalador numérico (scaler).
    """
    path = Path("data_output/models")
    try:
        model = joblib.load(path / "regression_model.pkl")
        encoder = joblib.load(path / "target_encoder.pkl")
        scaler = joblib.load(path / "scaler.pkl")
        return model, encoder, scaler
    except FileNotFoundError as e:
        return None, None, None


@st.cache_data
def get_default_ccaa_values(df):
    """
    Calcula la media de los indicadores del año 2023 agrupados por Comunidad Autónoma.
    Esto se usa para rellenar los valores por defecto de los sliders en la interfaz,
    ofreciendo un punto de partida realista al usuario.
    """
    return (
        df[df["anio"] == 2023]
        .groupby("comunidad")
        .agg(
            {
                "salario_medio": "mean",
                "tasa_paro_media": "mean",
                "Comunicaciones": "mean",
                "Restaurantes y hoteles": "mean",
                "Alimentos y bebidas no alcohólicas": "mean",
                "Bebidas alcohólicas y tabaco": "mean",
                "Sanidad": "mean",
                "Transporte": "mean",
                "Vestido y calzado": "mean",
                "Otros bienes y servicios": "mean",
                "Muebles, artículos del hogar y artículos para el mantenimiento corriente del hogar": "mean",
                "Enseñanza": "mean",
                "Ocio y cultura": "mean",
            }
        )
        .round(2)
        .to_dict(orient="index")
    )


# ── VISTA PRINCIPAL (UI) ───────────────────────────────────────────────────


def show_simulator():
    """
    Función principal que renderiza la pestaña del Simulador Predictivo.
    Contiene la interfaz de usuario, la carga dinámica de variables y la predicción.
    """
    # 1. Cabecera e introducción
    st.header("Simulador de Precios de Vivienda (IA)")
    st.info(
        "Modifica los indicadores socioeconómicos y el modelo de **Gradient Boosting** "
        "estimará el Índice de Precios de la Vivienda (IPV)."
    )

    st.subheader("Fiabilidad del Modelo")

    # 2. Tarjetas de métricas del modelo (KPIs)
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)

    # Leemos las métricas reales del entrenamiento guardadas en el JSON
    metrics_path = Path("data_output/models/model_metrics.json")

    if metrics_path.exists():
        with open(metrics_path, "r", encoding="utf-8") as f:
            metrics = json.load(f)
    else:
        # Fallback de seguridad si no existe el archivo
        metrics = {
            "Modelo": "Gradient Boosting",
            "R2_Test": 0.84,
            "MAE_Test": 6.01,
            "RMSE_Test": 7.60,
        }

    col_m1.metric(
        label="Modelo utilizado",
        value=f"**{metrics['Modelo']}**",
        help=(
            "Algoritmo de aprendizaje automático que construye árboles de decisión "
            "de forma secuencial, donde cada árbol corrige los errores del anterior. "
            "Fue el ganador de un torneo automático (AutoML) frente a Ridge, K-NN y Random Forest."
        ),
        border=True,
    )
    col_m2.metric(
        label="R² en Test",
        value=f"{metrics["R2_Test"]:.2f}",
        help=(
            "Coeficiente de determinación. Mide qué porcentaje de la variación real "
            "del precio de la vivienda explica el modelo. Un valor de 0.84 significa "
            "que el modelo captura el 84% del comportamiento real de los datos, "
            "evaluado sobre datos que nunca vio durante el entrenamiento."
        ),
        border=True,
    )
    col_m3.metric(
        label="MAE",
        value=f"{metrics['MAE_Test']:.2f} pts",
        help=(
            "Error Absoluto Medio (Mean Absolute Error). En promedio, la predicción "
            "del modelo se desvía ±6.01 puntos del IPV real. "
            "Cuanto más bajo, más preciso es el modelo."
        ),
        border=True,
    )
    col_m4.metric(
        label="RMSE",
        value=f"{metrics['RMSE_Test']:.2f} pts",
        help=(
            "Error Cuadrático Medio (Root Mean Squared Error). Similar al MAE pero "
            "penaliza más los errores grandes. Un valor de 7.60 indica que los fallos "
            "puntuales del modelo son pequeños y no hay predicciones muy descabelladas."
        ),
        border=True,
    )

    # 3. Gráficos HTML exportados desde la fase de entrenamiento
    with st.expander("📊 Ver fiabilidad del modelo (Realidad vs Predicción)"):
        html_path = Path("data_output/graphics/grafico_prediccion_vs_real.html")
        if html_path.exists():
            st.iframe(html_path.read_text(encoding="utf-8"), height=450)
        else:
            st.warning("No se encontró el gráfico en data_output/graphics/")

    st.subheader("¿Qué variables mueven la predicción? - Importancia del Modelo")

    html_fi = Path("data_output/graphics/grafico_importancia_rf.html")
    if html_fi.exists():
        st.iframe(html_fi.read_text(encoding="utf-8"), height="content")
        st.info(
            "Las variables que más impactan son: **Comunicaciones y Restaurante y hoteles**. Tenlo en cuenta a la hora de predecir el precio de la vivienda. "
        )

    else:
        st.warning("No se encontró el gráfico. Comprueba la ruta.")

    st.divider()

    # 4. Carga de los archivos del modelo en memoria
    model, target_encoder, scaler = load_model()

    if model is None:
        st.error(
            "⚠️ Faltan los archivos del modelo. "
            "Asegúrate de que existan: `data_output/models/regression_model.pkl`, "
            "`target_encoder.pkl` y `scaler.pkl`."
        )
        st.stop()  # Detiene la ejecución si no hay modelo

    # 5. Formulario interactivo (Inputs del usuario)
    st.subheader("⚙️ Crea tu escenario")
    st.markdown("**Datos principales**")

    # Preparamos los datos base para los inputs
    historic_df = load_historic_data()
    ccaa_defaults = get_default_ccaa_values(historic_df)
    expected_model_columns = list(
        scaler.feature_names_in_
    )  # Columnas exactas que espera el modelo
    unique_ccaa_list = sorted(historic_df["comunidad"].dropna().unique().tolist())

    col1, col2 = st.columns(2)
    selected_data = (
        {}
    )  # Diccionario donde guardaremos todo lo que el usuario seleccione

    # --- Columna Izquierda: Datos Macro ---
    with col1:
        with st.container(border=True):
            ccaa_input = st.selectbox("Comunidad Autónoma", unique_ccaa_list)
            selected_data["comunidad"] = ccaa_input

            # Obtenemos los valores por defecto de la CCAA seleccionada
            v = ccaa_defaults.get(ccaa_input, {})
            monthly_salary = int(v.get("salario_medio", 28000) / 12)
            default_unemployment = float(v.get("tasa_paro_media", 12.0))

            st.markdown("**Variables principales**")
            salary_input = st.slider(
                "Salario mensual bruto (€)", 1_000, 4_000, monthly_salary, step=50
            )
            selected_data["salario_medio"] = (
                salary_input * 12
            )  # Anualizamos para el modelo

            unemployment_input = st.slider(
                "Tasa de paro (%)", 5.0, 35.0, default_unemployment, step=0.5
            )
            selected_data["tasa_paro_media"] = unemployment_input

            st.caption(f"Valores pre-rellenados para **{ccaa_input}** (media 2023).")

    # --- Columna Derecha: Categorías del IPC ---
    with col2:
        with st.container(border=True):
            st.markdown("**IPC por categorías (Destacadas)**")

            ipc_com_default = float(v.get("Comunicaciones", 100.0))
            ipc_rest_default = float(v.get("Restaurantes y hoteles", 105.0))
            ipc_ali_default = float(v.get("Alimentos y bebidas no alcohólicas", 110.0))

            selected_data["Comunicaciones"] = st.slider(
                "Comunicaciones — peso 0.39",
                80.0,
                130.0,
                ipc_com_default,
                step=0.5,
                help="Incluye servicios de telefonía, conexión a internet y compra de equipos móviles.",
            )
            selected_data["Restaurantes y hoteles"] = st.slider(
                "Restaurantes y hoteles — peso 0.25",
                80.0,
                130.0,
                ipc_rest_default,
                step=0.5,
                help="Evolución de precios en menús del día, cafeterías, servicios de alojamiento y hoteles.",
            )
            selected_data["Alimentos y bebidas no alcohólicas"] = st.slider(
                "Alimentos — peso 0.02",
                80.0,
                130.0,
                ipc_ali_default,
                step=0.5,
                help="Refleja el coste de la cesta de la compra básica en supermercados (pan, carne, aceite, frutas, etc.).",
            )

        # Generador dinámico para el resto de categorías menos importantes
        with st.expander("Resto de categorías IPC"):
            showed_variables = list(selected_data.keys())
            rest_ipc_categories = [
                col for col in expected_model_columns if col not in showed_variables
            ]

            help_dict = {
                "Transporte": "Precio de carburantes, compra de vehículos y transporte público (metro, autobús, vuelos).",
                "Sanidad": "Servicios médicos, seguros de salud, dentistas y productos farmacéuticos.",
                "Ocio y cultura": "Entradas a espectáculos, cine, libros, paquetes turísticos y equipos audiovisuales.",
                "Vestido y calzado": "Prendas de vestir y zapatos (suele fluctuar mucho por la temporada de rebajas).",
                "Enseñanza": "Matrículas universitarias, colegios privados, academias y material escolar.",
                "Muebles, artículos del hogar y artículos para el mantenimiento corriente del hogar": "Electrodomésticos, muebles, herramientas y artículos de limpieza diaria.",
                "Bebidas alcohólicas y tabaco": "Vinos, cervezas, licores y cigarrillos (muy afectado por impuestos especiales).",
                "Otros bienes y servicios": "Seguros de coche/hogar, peluquerías, residencias y cuidados personales.",
            }
            col_expander = st.columns(3)
            for i, cat in enumerate(rest_ipc_categories):
                with col_expander[i % 3]:  # Distribuye uniformemente en las 3 columnas
                    default_value = float(v.get(cat, 100.0))
                    label_name = (
                        "Muebles y hogar" if "Muebles" in cat else cat
                    )  # Acorta el nombre largo
                    selected_data[cat] = st.number_input(
                        label_name,
                        value=default_value,
                        step=0.5,
                        help=help_dict.get(
                            cat, "Índice de precios para esta categoría."
                        ),
                    )

    st.divider()

    # Variable fijada para el análisis del Scatter Plot interactivo
    scatter_variable = "Comunicaciones"

    # 6. GESTIÓN DEL BOTÓN Y MEMORIA DE ESTADO (Session State)

    # Si el usuario ha modificado cualquier valor del formulario,
    # ocultamos la predicción
    if "saved_data" in st.session_state:
        if selected_data != st.session_state.saved_data:
            st.session_state.show_prediction = False

    # El botón solo se encarga de guardar los datos en memoria.
    # Así, si cambiamos de pestaña, la predicción no desaparece.
    if st.button("🔮 Calcular IPV estimado", type="primary", width="stretch"):
        st.session_state.show_prediction = True
        st.session_state.saved_data = selected_data

    # 7. BLOQUE DE PREDICCIÓN (Solo se ejecuta si la memoria está activa)
    if st.session_state.get("show_prediction", False):
        st.subheader("Resultado e Interpretación")

        # Calculamos la media nacional real para comparar
        try:
            historic_df = load_historic_data()
            national_mean = historic_df["precio_vivienda"].mean()
        except Exception:
            historic_df = None
            national_mean = 112.1  # Fallback por si hay un error al leer el CSV

        # Transformación y Predicción con el modelo
        # Usamos los datos guardados en memoria para construir el DataFrame
        prediction_df = pd.DataFrame(
            [st.session_state.saved_data], columns=expected_model_columns
        )
        try:
            encoded_data = target_encoder.transform(prediction_df)
            scaled_data = scaler.transform(encoded_data)
            predicted_ipv = model.predict(scaled_data)[0]
        except Exception as e:
            st.error(f"Error en la transformación: {e}")
            st.stop()

        col_m1, col_g2 = st.columns(2)

        # --- Resultados en Texto (Columna Izquierda) ---
        with col_m1:
            st.metric(
                label="Índice de Precio de Vivienda estimado (Base 100 = referencia INE)",
                value=f"{predicted_ipv:.1f}",
                delta=(predicted_ipv - national_mean).round(2),
                delta_color="inverse",
                border=True,
            )

            # Generación del mensaje dinámico contextualizado
            base_diff = predicted_ipv - 100

            if predicted_ipv > national_mean:
                mean_txt = f"**por encima de la media nacional ({national_mean:.1f})**"
                # Usamos warning (amarillo) si el precio es mayor
                st.warning(
                    f"El IPV estimado de **{predicted_ipv:.1f}** está {mean_txt}, "
                    f"y un **{base_diff:.1f}% por encima del año base (100)**. "
                    "El esfuerzo financiero para acceder a la vivienda en este escenario es crítico."
                )
            else:
                mean_txt = f"**por debajo de la media nacional ({national_mean:.1f})**"
                # Usamos info (azul) si se mantiene
                st.info(
                    f"El IPV estimado de **{predicted_ipv:.1f}** está {mean_txt}, "
                    f"pero un **{base_diff:.1f}% por encima del año base (100)**. "
                    "Aunque está por debajo de la media, la vivienda sigue encareciéndose."
                )

        # --- Gráfico Contextual Histograma (Columna Derecha) ---
        with col_g2:
            if historic_df is not None:
                with st.container(border=True):

                    # Histograma
                    fig_hist = px.histogram(
                        historic_df,
                        x="precio_vivienda",
                        nbins=30,
                        title="Distribución histórica del IPV + tu escenario",
                        labels={"precio_vivienda": "IPV"},
                        color_discrete_sequence=["#97C2FC"],
                    )
                    fig_hist.add_vline(
                        x=predicted_ipv,
                        line_dash="solid",
                        line_color="red",
                        line_width=2,
                        annotation_text="tu escenario",
                        annotation_font_color="red",
                    )
                    fig_hist.update_layout(margin=dict(t=40, b=10))

                    st.plotly_chart(
                        fig_hist,
                        width="stretch",
                        height=230,
                    )
                    st.caption(
                        "💡 **Cómo leer este gráfico:** Las barras azules muestran la frecuencia histórica de los precios de vivienda en España. La línea roja indica dónde se sitúa tu escenario. Si cae en un extremo, significa que predice un mercado inmobiliario inusualmente barato o caro."
                    )

        # --- Gráfico Inferior a todo ancho: Scatter Plot---
        if historic_df is not None:
            with st.container(border=True):
                st.markdown("**Analiza tu escenario en perspectiva**")
                
                # El usuario elige con qué variable comparar el IPV
                scatter_options = [
                    col for col in expected_model_columns if col != "comunidad"
                ]
                selected_scatter_axis = st.selectbox(
                    "Elige la métrica para comparar en el gráfico:",
                    scatter_options,
                    index=(
                        scatter_options.index("Comunicaciones")
                        if "Comunicaciones" in scatter_options
                        else 0
                    ),
                )

                x_coord_value = prediction_df[selected_scatter_axis].iloc[0]

                scatter_fig = px.scatter(
                    historic_df,
                    x=selected_scatter_axis,
                    y="precio_vivienda",
                    color="comunidad",
                    opacity=0.45,
                    title=f"Impacto de {selected_scatter_axis} en el IPV",
                    labels={
                        selected_scatter_axis: selected_scatter_axis,
                        "precio_vivienda": "IPV",
                    },
                )
                scatter_fig.add_scatter(
                    x=[x_coord_value],
                    y=[predicted_ipv],
                    mode="markers+text",
                    marker=dict(
                        color="red",
                        size=16,
                        symbol="star",
                        line=dict(color="black", width=1),
                    ),
                    text=["Tu escenario"],
                    textposition="top center",
                    textfont=dict(color="red", size=11),
                    name="Predicción",
                    showlegend=False,
                )
                scatter_fig.update_layout(margin=dict(t=40, b=10))
                st.plotly_chart(scatter_fig, width="stretch")
                st.caption(
                    "💡 **Cómo leer esto:** Cada punto es un registro histórico. Si tu estrella roja se aleja de la 'nube', tu escenario plantea una situación económica que rara vez ha ocurrido en la realidad."
                )
