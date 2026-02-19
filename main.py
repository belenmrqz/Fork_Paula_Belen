import sys

# --- Imports de la Fase 1 (API -> SQLite) ---
from config.constantes import (
    IPC,
    IPV,
    TASA_PARO,
    TEMPORALIDAD,
    EAES_OCUPACION,
    EAES_PERCENTILES,
    ETCL,
)
from src.inedata import INEDataExtractor
from src.procesar import procesar_datos
from src.almacenar import insertar_datos
from src.db import DatabaseConnection, crear_base_datos

# --- Imports de la Fase 2 y 3 (Polars y Plotly) ---
from analysis.transform import process_data_polars
from analysis.visualize import generate_plotly_charts

def etl_fase1_extraccion():

    crear_base_datos()

    tablas = [IPC, IPV, TASA_PARO, TEMPORALIDAD, EAES_OCUPACION, EAES_PERCENTILES, ETCL]
    for codigo in tablas:
        extractor = INEDataExtractor(codigo)
        if extractor.obtener_datos():
            datos_procesados = procesar_datos(codigo, extractor.raw_data)
            
            print("Procesando datos de tabla (Mostrando la primera fila)", codigo)
            print(datos_procesados[0])

            print("Número de filas a insertar", len(datos_procesados))

            tabla_destino = ""

            if codigo in [IPC, IPV]:
                tabla_destino = "T_precios"
            elif codigo in [ETCL, EAES_OCUPACION, EAES_PERCENTILES]:
                tabla_destino = "T_salarios"
            elif codigo in [TASA_PARO, TEMPORALIDAD]:
                tabla_destino = "T_empleo"

            # Llamamos a almacenar pasándole el nombre
            if tabla_destino and datos_procesados:
                insertar_datos(tabla_destino, datos_procesados)
                pass
            # ---------------------------------------------------------
        else:
            print(f"No se pudieron obtener los datos de la tabla {codigo}")

    DatabaseConnection().close()

def menu():
    """
    Menú interactivo de terminal para orquestar todo el pipeline de datos.
    """
    while True:
        print("\n" + "="*50)
        print(" PANEL DE CONTROL - PROYECTO DATOS INE")
        print("="*50)
        print("1. Descargar y actualizar Base de Datos (ETL - Fase 1)")
        print("2. Procesar Datasets con Polars         (ETL - Fase 2)")
        print("3. Generar Gráficos con Plotly          (ETL - Fase 3)")
        print("4. Ejecutar Pipeline Completo           (Todo a la vez)")
        print("5. Salir")
        print("="*50)
        
        opcion = input("Elige una opción (1-5): ")
        
        if opcion == '1':
            etl_fase1_extraccion()
        
        elif opcion == '2':
            process_data_polars()
            
        elif opcion == '3':
            generate_plotly_charts()
            
        elif opcion == '4':
            print("\nINICIANDO PIPELINE COMPLETO...")
            etl_fase1_extraccion()
            process_data_polars()
            generate_plotly_charts()
            print("\n¡PIPELINE COMPLETO FINALIZADO CON ÉXITO!")
            
        elif opcion == '5':
            print("\n¡Hasta pronto!")
            sys.exit()
            
        else:
            print("\nOpción no válida. Inténtalo de nuevo.")


if __name__ == "__main__":
    menu()