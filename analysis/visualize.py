import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# Configuración de rutas
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
data_dir = os.path.join(project_root, "data_output")

def generar_visualizaciones():
    # 1. Cargar los datos generados
    df_salarios = pl.read_csv(os.path.join(data_dir, "Evolucion_Salario_Comunidades.csv"))
    df_poder = pl.read_csv(os.path.join(data_dir, "Relacion_Poder_Adquisitivo.csv"))

    # --- GRÁFICO 1: Evolución del salario por comunidad autonoma (Interactivo) ---
    # Este gráfico permite ver cómo han crecido los salarios en cada sitio
    fig1 = px.line(
        df_salarios, 
        x="anio", 
        y="salario_medio", 
        color="comunidad",
        title="Evolución del Salario Medio Anual por CCAA",
        markers=True,
        labels={"salario_medio": "Euros (€)", "anio": "Año"}
    )
    fig1.write_html(os.path.join(data_dir, "1_evolucion_salarios.html"))

    print("Gráfico generado en la carpeta data_output")
    fig1.show()

if __name__ == "__main__":
    generar_visualizaciones()