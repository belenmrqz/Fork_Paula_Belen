import streamlit as st
from pathlib import Path
from config.constantes import GRAPHICS_DIR


def render_html(filename: str, height="content") -> None:
    """
    Renderiza un archivo HTML de Plotly exportado.
    Acepta nombre de archivo relativo a GRAPHICS_DIR o ruta absoluta.
    """
    path = Path(filename) if Path(filename).is_absolute() else GRAPHICS_DIR / filename
    if path.exists():
        st.iframe(path.read_text(encoding="utf-8"), height=height, width="stretch")
    else:
        st.warning(f"Gráfico no encontrado: {path}")


def chart_caption(text: str) -> None:
    """Caption estándar con el prefijo de bombilla."""
    st.caption(f"💡 {text}")
