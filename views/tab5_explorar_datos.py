import streamlit as st
import pandas as pd
from pathlib import Path

# Catálogo de datasets: nombre legible -> archivo CSV + descripción
DATASETS = {
    "💼 Evolución Salarial por CCAA": {
        "file": "Evolucion_Salario_Comunidades.csv",
        "desc": "Salario medio anual por Comunidad Autónoma y año. Fuente: EAES (INE Tabla 28191).",
    },
    "📈 Salario Nominal vs Real": {
        "file": "Salario_Nominal_vs_Real.csv",
        "desc": "Comparativa del salario bruto de nómina frente al salario deflactado por el IPC. Muestra la ilusión monetaria.",
    },
    "🏠 Comparativa Vivienda vs Salarios": {
        "file": "Comparativa_Vivienda_Salario.csv",
        "desc": "Evolución del IPV y el salario medio en Base 100 (2015). Permite ver qué crece más rápido.",
    },
    "⚖️ Brecha Salarial por Ocupación": {
        "file": "Brecha_Salarial_Ocupacion.csv",
        "desc": "Diferencia porcentual entre el salario de hombres y mujeres desglosada por tipo de ocupación (CNO-11).",
    },
    "📉 Paro vs Salarios por CCAA": {
        "file": "Relacion_Paro_Salarios.csv",
        "desc": "Cruce entre tasa de paro media y salario medio por región y año. Base de la Curva de Phillips regional.",
    },
    "🔗 Correlación Paro-Salarios (Pearson)": {
        "file": "Correlacion_Paro_Salarios.csv",
        "desc": "Coeficiente de correlación de Pearson entre paro y salario por Comunidad Autónoma.",
    },
    "👷 Calidad del Empleo (Temporalidad)": {
        "file": "Calidad_Empleo.csv",
        "desc": "Proporción de contratos indefinidos vs temporales a nivel nacional. Incluye el efecto de la reforma laboral de 2022.",
    },
    "📊 Desigualdad Salarial": {
        "file": "Desigualdad_Salarial.csv",
        "desc": "Evolución de la media, mediana, percentil 10 y cuartil inferior del salario en España.",
    },
    "🤖 Dataset ML (Regresión IPV)": {
        "file": "ML.csv",
        "desc": "Dataset consolidado usado para entrenar los modelos predictivos. Cruza vivienda, salarios, paro e IPC por categorías.",
    },
}

# Diccionario de mapeo para mejorar la legibilidad de las columnas en la interfaz
COLUMN_LABELS = {
    "anio": "Año",
    "comunidad": "Comunidad Autónoma",
    "salario_medio": "Salario medio (€)",
    "salario_nominal": "Salario nominal (€/mes)",
    "salario_real": "Salario real (€/mes)",
    "ipc_valor": "IPC anual (índice)",
    "poder_adquisitivo": "Poder adquisitivo",
    "ipv": "Índice Precio Vivienda",
    "indice_salario": "Índice Salario (Base 100)",
    "tasa_paro_media": "Tasa de paro (%)",
    "Hombres": "Salario hombres (€)",
    "Mujeres": "Salario mujeres (€)",
    "brecha_porcentual": "Brecha salarial (%)",
    "ocupacion": "Ocupación",
    "sexo": "Sexo",
    "indicador": "Indicador",
    "salario": "Salario (€)",
    "correlacion_pearson": "Correlación de Pearson",
    "Asalariados_Total": "Asalariados totales (miles)",
    "Asalariados_Temporal": "Asalariados temporales (miles)",
    "Temporal (%)": "Tasa temporalidad (%)",
    "Indefinido (%)": "Tasa indefinidos (%)",
    "precio_vivienda": "IPV regional",
}


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recorre las columnas del DataFrame y las renombra si existen en el diccionario COLUMN_LABELS.
    Esto permite mantener el código interno limpio pero mostrar etiquetas amigables al usuario.
    """
    return df.rename(
        columns={c: COLUMN_LABELS[c] for c in df.columns if c in COLUMN_LABELS}
    )


# Ruta global de los datos procesados
CSV_DIRECTORY = Path("data_output/csv")


@st.cache_data
def load_dataset(filename: str) -> pd.DataFrame:
    """
    Carga el archivo CSV y lo guarda en caché para mejorar el rendimiento
    al cambiar entre diferentes vistas o filtros.
    """
    return pd.read_csv(CSV_DIRECTORY / filename)


@st.dialog("Opciones de descarga")
def download_options_dialog(filtered_df, original_df, filename):
    """
    Ventana emergente para decidir qué versión del CSV descargar.
    """
    st.write(f"Has seleccionado el dataset: **{filename}**")
    st.info("¿Qué versión del archivo prefieres?")

    col_dialog1, col_dialog2 = st.columns(2)

    with col_dialog1:
        st.subheader("Vista Filtrada")
        st.write(f"Registros: {len(filtered_df)}")
        csv_filtered = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Descargar Filtrado",
            data=csv_filtered,
            file_name=f"filtered_{filename}",
            mime="text/csv",
            use_container_width=True,
            on_click=st.rerun,  # Cerramos el diálogo tras la acción
        )

    with col_dialog2:
        st.subheader("Dataset Completo")
        st.write(f"Registros: {len(original_df)}")
        csv_original = original_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Descargar Original",
            data=csv_original,
            file_name=f"full_{filename}",
            mime="text/csv",
            use_container_width=True,
            on_click=st.rerun,
        )


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera filtros automáticos según el tipo de cada columna:
    - Texto/categoría  → checkboxes en contenedor con scroll.
    - Numérica         → slider de rango.
    No procesa columnas con demasiada cardinalidad (ej. IDs o salarios exactos).
    """
    filtered_df = df.copy()

    # Columnas que nunca filtramos por tener demasiados valores únicos o ser irrelevantes
    EXCLUDE_COLUMNS = {
        "Año",
        "Salario (€)",
        "Salario medio (€)",
        "Salario nominal (€/mes)",
        "Salario real (€/mes)",
        "IPC anual (índice)",
        "Poder adquisitivo",
        "Índice Precio Vivienda",
        "Índice Salario (Base 100)",
        "IPV regional",
    }

    # Selección automática de columnas para filtrar (Texto < 25 categorías)
    text_columns = [
        col
        for col in filtered_df.columns
        if filtered_df[col].dtype == object
        and col not in EXCLUDE_COLUMNS
        and filtered_df[col].nunique() <= 25
    ]

    # Selección automática de columnas numéricas (excluyendo años)
    numeric_columns = [
        col
        for col in filtered_df.columns
        if pd.api.types.is_numeric_dtype(filtered_df[col])
        and col not in EXCLUDE_COLUMNS
        and col != "Año"
        and filtered_df[col].nunique() > 2
    ]

    if not text_columns and not numeric_columns:
        return filtered_df

    with st.expander("🔍 Filtros y búsqueda", expanded=False):

        # 1. BUSCADOR GLOBAL: Filtra filas que contengan el texto en cualquier celda
        search_query = st.text_input(
            "Buscar en la tabla", placeholder="Escribe para filtrar filas..."
        )
        if search_query:
            # Creamos una máscara booleana buscando el texto en todo el dataframe
            search_mask = filtered_df.apply(
                lambda col: col.astype(str).str.contains(
                    search_query, case=False, na=False
                )
            ).any(axis=1)
            filtered_df = filtered_df[search_mask]

        # 2. FILTROS NUMÉRICOS (sliders): Se distribuyen en columnas (máximo 3 por fila)
        if numeric_columns:
            num_cols_layout = st.columns(min(len(numeric_columns), 3))
            for i, col_name in enumerate(numeric_columns):
                min_val = float(filtered_df[col_name].min())
                max_val = float(filtered_df[col_name].max())

                if min_val < max_val:
                    selected_range = num_cols_layout[i % 3].slider(
                        col_name, min_val, max_val, (min_val, max_val), format="%.1f"
                    )
                    filtered_df = filtered_df[
                        filtered_df[col_name].between(
                            selected_range[0], selected_range[1]
                        )
                    ].reset_index(drop=True)

        # 3. FILTROS CATEGÓRICOS: Checkboxes con gestión de estado global
        if text_columns:
            for col_name in text_columns:
                st.write(f"**Filtrar por {col_name}**")
                options = sorted(filtered_df[col_name].dropna().unique().tolist())
                total_options = len(options)

                # Gestión de estado para botones de selección masiva
                for opt in options:
                    state_key = f"chk_{col_name}_{opt}"
                    if state_key not in st.session_state:
                        st.session_state[state_key] = True

                # Cálculo de cuántos elementos hay seleccionados actualmente
                checked_count = sum(
                    [st.session_state[f"chk_{col_name}_{opt}"] for opt in options]
                )

                # Botones de acción masiva
                btn_col1, btn_col2, btn_spacer = st.columns([1, 1, 3])

                if checked_count < total_options:
                    if btn_col1.button("Seleccionar todo", key=f"all_{col_name}"):
                        for opt in options:
                            st.session_state[f"chk_{col_name}_{opt}"] = True
                        st.rerun()  # Recarga para aplicar el cambio visual de los checkboxes

                if checked_count > 0:
                    if btn_col2.button("Quitar todo", key=f"none_{col_name}"):
                        for opt in options:
                            st.session_state[f"chk_{col_name}_{opt}"] = False
                        st.rerun()

                # Contenedor con scroll para evitar que la UI crezca demasiado
                with st.container(height=150):
                    selected_options = []
                    for opt in options:
                        if st.checkbox(opt, key=f"chk_{col_name}_{opt}"):
                            selected_options.append(opt)

                # Aplicación del filtro al DataFrame
                if selected_options:
                    filtered_df = filtered_df[
                        filtered_df[col_name].isin(selected_options)
                    ].reset_index(drop=True)
                else:
                    # Si no hay nada seleccionado, el resultado es un dataframe vacío
                    filtered_df = filtered_df.iloc[0:0]

    return filtered_df


def show_explore_data():
    """
    Lógica principal de la pestaña de exploración.
    """
    st.title("🗄️ Explorar datos")
    st.markdown(
        "Datasets de la **Capa Oro** generados por el pipeline ETL · Listos para análisis"
    )

    # Selector de dataset: El usuario elige una de las llaves del diccionario DATASETS
    selected_dataset_name = st.selectbox(
        "Selecciona un dataset",
        list(DATASETS.keys()),
        help="Todos los datasets están desnormalizados y limpios. Son el resultado final del pipeline Polars.",
    )
    meta = DATASETS[selected_dataset_name]
    file_path = CSV_DIRECTORY / meta["file"]

    # Muestra información técnica del archivo debajo del selector
    st.caption(f"📁 `data_output/csv/{meta['file']}` · {meta['desc']}")

    # Verificación de seguridad: si el archivo no existe, avisa al usuario
    if not file_path.exists():
        st.error(
            f"No se encontró el archivo: {meta['file']}. Ejecuta primero el pipeline ETL (Opción 2 del main.py)."
        )
        return

    # Carga y renombramiento de columnas
    raw_df = load_dataset(meta["file"])
    labeled_df = rename_columns(raw_df)

    # Contenedor para KPIs (se declara antes de los filtros pero se llena después)
    kpi_placeholder = st.container()

    # Lógica de filtrado
    final_df = labeled_df.copy()

    # Filtro especial de Año (Slider principal)
    if "Año" in final_df.columns:
        years = sorted(final_df["Año"].dropna().unique().astype(int).tolist())
        year_rng = st.select_slider(
            "Filtrar por rango de años", options=years, value=(years[0], years[-1])
        )
        final_df = final_df[
            final_df["Año"].between(year_rng[0], year_rng[1])
        ].reset_index(drop=True)

    # Aplicar filtros dinámicos (UI expandible)
    final_df = render_filters(final_df)

    # Renderizado de KPIs reactivos
    with kpi_placeholder:
        c1, c2, c3, c4 = st.columns(4)
        # Filas
        c1.metric("Registros", f"{len(final_df):,}".replace(",", "."), border=True)

        # Columnas
        c2.metric("Variables", len(final_df.columns), border=True)

        # Periodo
        y_range = (
            f"{int(final_df['Año'].min())}–{int(final_df['Año'].max())}"
            if ("Año" in final_df.columns and not final_df.empty)
            else "—"
        )
        c3.metric("Periodo", y_range, border=True)

        # Cálculo del tamaño del archivo dinámicamente
        csv_data = final_df.to_csv(index=False).encode("utf-8")
        size_kb = len(csv_data) / 1024
        size_text = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
        c4.metric("Tamaño CSV", size_text, border=True)

    # ── Tabla interactiva ────────────────────────────────────────────────
    st.dataframe(
        final_df,
        width="stretch",
        hide_index=True,
    )

    st.divider()

    # ── Sección de Descargas ──────────────────────────────────────────────
    st.subheader("📥 Exportar información")

    download_col1, download_col2 = st.columns(2)

    with download_col1:
        with st.container(border=True):
            st.markdown("**Formato CSV**")
            st.caption("Ideal para Excel o Google Sheets.")
            # En lugar de descargar directo, abrimos el diálogo
            if st.button("Configurar descarga CSV", use_container_width=True):
                download_options_dialog(final_df, labeled_df, meta["file"])

    with download_col2:
        with st.container(border=True):
            st.markdown("**Formato Parquet (Solo original)**")
            st.caption("Archivo comprimido de alto rendimiento.")

            parquet_path = Path("data_output/parquet") / meta["file"].replace(
                ".csv", ".parquet"
            )
            if parquet_path.exists():
                st.download_button(
                    label="Descargar Parquet Original",
                    data=parquet_path.read_bytes(),
                    file_name=meta["file"].replace(".csv", ".parquet"),
                    mime="application/octet-stream",
                    use_container_width=True,
                )
            else:
                st.button(
                    "Parquet no disponible", disabled=True, use_container_width=True
                )
