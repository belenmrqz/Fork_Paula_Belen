import streamlit as st
from views.clustering_logic import show_clustering
from views.simulator_logic import show_simulator 

# from views.clustering_logic import show_clustering 

def show_modelos_ml():
    """Renderiza la pestaña principal de Modelos de Machine Learning."""
    
    st.title("🤖 Modelos ML")
    st.markdown("Segmentación territorial y predicción del precio de la vivienda.")
    
    # Creamos las dos pestañas superiores para separar los modelos
    tab_cluster, tab_simulator = st.tabs(["🗂️ Clustering · K-Means", "🔮 Simulador IPV"])
    
    with tab_cluster:
        show_clustering() 
        
    with tab_simulator:
        show_simulator()