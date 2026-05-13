import streamlit as st

# 1. Configuración de la página
st.set_page_config(
    page_title="Dashboard | Poder Adquisitivo",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 2. Importación de las vistas
from views.tab1_inicio import show_home
from views.tab2_poder_adquisitivo import show_purchasing_power
from views.tab3_analisis_territorial import show_analisis_territorial
from views.tab4_mercado_laboral import show_mercado_laboral
from views.tab5_explorar_datos import show_explore_data
from views.tab6_modelos_ml import show_modelos_ml
from views.tab7_info_proyecto import show_project_info

TABS = {
    "🏠 Inicio":               show_home,
    #"💰 Poder Adquisitivo":    show_purchasing_power,
    "🗺️ Análisis Territorial": show_analisis_territorial,
    "👷 Mercado Laboral":      show_mercado_laboral,
    "🗄️ Explorar datos":       show_explore_data,
    "🤖 Modelos ML":           show_modelos_ml,
    "ℹ️ Sobre el Proyecto":    show_project_info,
}

if "nav_pill" not in st.session_state:
    st.session_state["nav_pill"] = "🏠 Inicio"


# --- SIDEBAR (Menú lateral) ---
with st.sidebar:
    # Cabecera del menú 
    st.markdown("### 📈 Poder Adquisitivo")
    st.caption("España · INE Data Pipeline")
    st.divider()

    # Navegación
    selected = st.pills(
        "Navegación",
        list(TABS.keys()),
        selection_mode="single",
        label_visibility="hidden",
        key="nav_pill", 
    )

    if selected is not None:
        st.session_state.active_tab = selected

    st.divider()
    st.caption("Desarrollado para el proyecto final de Sistemas de Big Data")

# --- CONTROLADOR DE VISTAS ---
active_tab = st.session_state.nav_pill or "🏠 Inicio"
TABS[active_tab]()
