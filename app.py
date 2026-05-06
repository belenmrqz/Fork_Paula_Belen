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
from views.tab2_poder_adquisitivo import show_poder_adquisitivo
from views.tab3_analisis_territorial import show_analisis_territorial
from views.tab4_mercado_laboral import show_mercado_laboral
from views.tab5_explorar_datos import show_explore_data
from views.tab6_modelos_ml import show_modelos_ml
from views.tab7_info_proyecto import show_project_info

# --- SIDEBAR (Menú lateral) ---
with st.sidebar:
    # Cabecera del menú 
    st.markdown("### 📈 Poder Adquisitivo")
    st.caption("España · INE Data Pipeline")
    st.divider()

    # Navegación
    opcion_seleccionada = st.pills(
        "Navegación",
        [
            "🏠 Inicio",
            "💰 Poder Adquisitivo",
            "🗺️ Análisis Territorial",
            "👷 Mercado Laboral",
            "🗄️ Explorar datos",
            "🤖 Modelos ML",
            "ℹ️ Sobre el Proyecto",
        ],
        default="🏠 Inicio",
        selection_mode="single",
        label_visibility="hidden",
    )

    st.divider()
    st.caption("Desarrollado para el proyecto final de Data Science.")

# --- CONTROLADOR DE VISTAS ---
# Según la opción del menú, ejecutamos la función de su archivo correspondiente
if opcion_seleccionada == "🏠 Inicio":
    show_home()

elif opcion_seleccionada == "💰 Poder Adquisitivo":
    show_poder_adquisitivo()

elif opcion_seleccionada == "🗺️ Análisis Territorial":
    show_analisis_territorial()

elif opcion_seleccionada == "👷 Mercado Laboral":
    show_mercado_laboral()

elif opcion_seleccionada == "🗄️ Explorar datos":
    show_explore_data()

elif opcion_seleccionada == "🤖 Modelos ML":
    show_modelos_ml()

elif opcion_seleccionada == "ℹ️ Sobre el Proyecto":
    show_project_info()
