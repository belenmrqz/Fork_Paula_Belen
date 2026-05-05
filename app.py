import streamlit as st
from views.tab1_context import show_context
from views.tab2_visualizations import show_visualizations
from views.tab3_clustering import show_clustering
from views.tab4_simulator import show_simulator

st.set_page_config(
    page_title="Poder Adquisitivo España",
    page_icon="📉",
    layout="wide"
)

st.title("📉 Impacto de la Inflación y Vivienda en el Poder Adquisitivo")
st.markdown("Análisis de la evolución socioeconómica en España · Datos INE 2002–2025")
st.divider()

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Contexto y datos",
    "📈 Visualizaciones",
    "🗺️ Radiografía regional",
    "🤖 Simulador IA"
])

with tab1:
    show_context()
with tab2:
    show_visualizations()
with tab3:
    show_clustering()
with tab4:
    show_simulator()