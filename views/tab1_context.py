import streamlit as st
def show_context():
    st.header("El Espejismo del Sueldo")
    st.write("A pesar del aumento del Salario Medio, el coste de vida ha mermado la capacidad de ahorro de los españoles. A continuación se muestra el dataset procesado que alimenta nuestro análisis:")
    
    # Aquí cargaremos las tablas
    try:
        st.info("💡 (Aquí aparecerán las tablas y los KPIS")
    except Exception as e:
        st.error(f"No se pudo cargar la tabla: {e}")
