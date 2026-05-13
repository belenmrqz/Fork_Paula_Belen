import streamlit as st
from config.constantes import REPO_URL

def show_project_info():
    """
    Renderiza la pestaña de información del proyecto, mostrando la arquitectura
    de la base de datos, las fuentes del INE (con enlaces) y el equipo desarrollador.
    """
    
    # --- ENCABEZADO ---
    st.title("ℹ️ Sobre el Proyecto")
    st.markdown("### Análisis de la Evolución del Poder Adquisitivo en España")
    
    # Botón principal para ir al repositorio
    st.link_button("🔗 Ver repositorio completo en GitHub", REPO_URL, type="primary")
    
    st.divider()

    # --- OBJETIVO Y ARQUITECTURA ---
    text_col, architecture_col = st.columns(2)
    
    with text_col:
        st.subheader("🎯 Objetivo y Fases")
        st.markdown("""
        El objetivo es analizar la evolución del poder adquisitivo de la clase trabajadora mediante un pipeline **End-to-End**:
        
        1. **Data Engineering (Fase 1):** Ingesta desde la API del INE, transformación y carga (ETL) a una base de datos SQLite.
        2. **Data Preparation (Fase 2):** Procesamiento en memoria de alto rendimiento y exportación a formatos analíticos mediante **Polars**.
        3. **Data Analytics & BI (Fase 3):** Generación de gráficas interactivas con **Plotly** y *Storytelling* de datos mediante **Tableau**.
        4. **Machine Learning (Fase 4):** Modelos de segmentación socioeconómica (K-Means) y predicción de precios (Gradient Boosting).
        """)
        
    with architecture_col:
        st.subheader("⚙️ Arquitectura de Datos")
        with st.container(border=True):
            st.markdown("**Flujo del Dato**")
            st.caption("API REST INE ➔ Python ➔ SQLite ➔ Polars ➔ Capa Oro ➔ Streamlit / Tableau / ML")
            
        with st.container(border=True):
            st.markdown("**Modelo en Estrella (Star Schema)**")
            st.markdown("""
            * **Dimensiones (Lookups):** Tiempo (`tbl_periodo`), Geografía (`tbl_geografia`), Variables (`tbl_indicador`).
            * **Hechos (Facts):** Precios (`T_precios`), Salarios (`T_salarios`), Empleo (`T_empleo`).
            """)

    st.divider()

    # --- FUENTES DE DATOS (Con enlaces al INE) ---
    st.subheader("🗄️ Fuentes de Datos (INE - API JSON-stat)")
    st.info("Peticiones automatizadas a las tablas oficiales del Instituto Nacional de Estadística. Haz clic en los enlaces para ver las fuentes originales.")
    
    # Usamos columnas para agrupar el detalle exhaustivo de las tablas
    source_col1, source_col2, source_col3 = st.columns(3)
    
    with source_col1:
        st.markdown("**🛒 Gasto y Coste de Vida**")
        st.markdown("- **IPC** - [Índice general y categorías](https://www.ine.es/jaxiT3/Tabla.htm?t=50913)")
        st.markdown("- **IPV** - [Índice de Precios de Vivienda](https://www.ine.es/jaxiT3/Tabla.htm?t=25171)")
        
    with source_col2:
        st.markdown("**💸 Ingresos y Salarios**")
        st.markdown("- **ETCL** - [Coste Salarial Bruto](https://www.ine.es/jaxiT3/Tabla.htm?t=6061)")
        st.markdown("- **EAES** - [Ganancia por trabajador](https://www.ine.es/jaxiT3/Tabla.htm?t=28191)")
        st.markdown("- **EAES** - [Ganancia por ocupación](https://www.ine.es/jaxiT3/Tabla.htm?t=28186)")
        
    with source_col3:
        st.markdown("**👷 Empleo**")
        st.markdown("- **EPA** - [Tasa de Paro](https://www.ine.es/jaxiT3/Tabla.htm?t=65334)")
        st.markdown("- **Asalariados** - [Por tipo de contrato](https://www.ine.es/jaxiT3/Tabla.htm?t=65132)")

    st.divider()

    # --- EQUIPO DE DESARROLLO ---
    st.subheader("🤝 Equipo de Desarrollo")
    
    st.markdown(
        "Este proyecto nació como un trabajo conjunto en su fase de extracción de datos, "
        "y fue bifurcado (*forked*) para su desarrollo analítico avanzado."
    )
    
    team_col1, team_col2 = st.columns(2)
    
    with team_col1:
        with st.container(border=True):
            st.markdown("**🚀 Autoras del Proyecto (Fases 1, 2, 3 y 4)**")
            st.caption("Desarrollo End-to-End: Extracción, Polars, Tableau, ML y Streamlit.")
            st.markdown("- [Belén Márquez López](https://github.com/belenmrqz)")
            st.markdown("- [Paula Sánchez Vélez](https://github.com/paulaschez)")
        
    with team_col2:
        with st.container(border=True):
            st.markdown("**🛠️ Colaboradores Iniciales (Fase 1)**")
            st.caption("Participación en el diseño inicial del ETL y la base de datos.")
            st.markdown("- [Alejandro Bernabé Guerrero](https://github.com/Alebernabe5)")
            st.markdown("- [Ivana Sánchez Pérez](https://github.com/Ivanasp43)")