import json
from pathlib import Path

import pandas as pd
import streamlit as st
import requests
from config.constantes import CSV_DIR, PARQUET_DIR, COLUMN_LABELS, GEOJSON_SPAIN, MODELS_DIR
import joblib

@st.cache_data
def load_csv(filename: str) -> pd.DataFrame:
    """Carga un CSV de la capa oro. Usa el nombre de archivo sin ruta."""
    return pd.read_csv(CSV_DIR / filename)

@st.cache_data
def load_json(path) -> dict:
    """Carga un JSON desde una ruta. Devuelve {} si no existe, sin lanzar error."""
    path = Path(path)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


@st.cache_data
def load_geojson() -> dict:
    """Descarga y cachea un GeoJSON remoto."""
    return requests.get(GEOJSON_SPAIN).json()

@st.cache_resource
def load_ml_artifacts(modelname: str, encodername: str, scalername: str):
    """Carga modelo, encoder y scaler de forma segura."""
    try:
        model = joblib.load(MODELS_DIR / modelname)
        encoder = joblib.load(MODELS_DIR / encodername)
        scaler = joblib.load(MODELS_DIR / scalername)
        return model, encoder, scaler
    except Exception:
        return None, None, None


def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas según COLUMN_LABELS. Deja intactas las no mapeadas."""
    return df.rename(
        columns={c: COLUMN_LABELS[c] for c in df.columns if c in COLUMN_LABELS}
    )


def get_last_year(df: pd.DataFrame, col: str = "anio") -> int:
    return int(df[col].max())


def load_global_css(file_path="assets/style.css"):
    """Carga el CSS global desde un archivo externo."""
    p = Path(file_path)
    if p.exists():
        css_content = p.read_text(encoding="utf-8")
        st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)
    else:
        st.warning(f"No se encontró el archivo CSS en {file_path}")