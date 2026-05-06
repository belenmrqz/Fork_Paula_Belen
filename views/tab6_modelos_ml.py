import streamlit as st

from views.simulator_logic import show_simulator 

# from views.clustering_logic import show_clustering 

def show_modelos_ml():
    """Renderiza la pestaña principal de Modelos de Machine Learning."""
    
    st.title("🤖 Modelos ML")
    st.markdown("Segmentación territorial y predicción del precio de la vivienda.")
    
    # Creamos las dos pestañas superiores para separar los modelos
    tab_cluster, tab_simulator = st.tabs(["🗂️ Clustering · K-Means", "🔮 Simulador IPV"])
    
    with tab_cluster:
        st.info("Segmentación no supervisada de las 19 CCAA usando **K-Means (k=3)**.")
        # show_clustering() 
        
        st.write("📍 *Aquí irá el mapa interactivo y las gráficas de Clustering.*")
        
    with tab_simulator:
        show_simulator()