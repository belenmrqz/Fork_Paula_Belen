# transform.py
# Paso 1 y 2: Conexión y limpieza con Polars

import polars as pl
import sqlite3
import os 
import sys

# # 1. Obtenemos la ruta absoluta de la carpeta donde está ESTE archivo (analysis)
# current_dir = os.path.dirname(os.path.abspath(__file__))

# # 2. Subimos un nivel para llegar a la raíz del proyecto
# project_root = os.path.dirname(current_dir)

# # 3. Construimos la ruta a la base de datos de forma absoluta
# db_path = os.path.join(project_root, "proyecto_datos.db")
# # Ruta a la base de datos

# 1. Aseguramos que Python encuentre la carpeta 'src' subiendo un nivel
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 2. Importamos vuestra conexión original
from src.db import DatabaseConnection

# método para establecer la conexión con la BBDD
# def establish_connection ():
#     try:
#         normalized_path = os.path.abspath(db_path) # Normalizamos la ruta para evitar errores visuales

#         if not os.path.exists(db_path): # Comprobar que existe
#             raise FileExistsError(f"No se encuentra el archivo {db_path}")
        
#         connection = sqlite3.connect(db_path) # Establecer conexión

#         query_test = "SELECT name FROM sqlite_master WHERE type='table';" # Prueba
#         tables = connection.execute(query_test).fetchall()


#         print("\nCONEXIÓN EXITOSA A LA BASE DE DATOS")
#         print(f"Ubicación -> {normalized_path}")
#         print("Tablas encontradas: ")
#         for table in tables:
#             print(f"   - {table[0]}")
        
#         return connection


#     # Posibles errores:
#     except FileNotFoundError as e:
#         print(f"Error de ruta: {e}")
#         return None
#     except sqlite3.Error as e:
#         print(f"Error de SQLite al conectar: {e}")
#         return None
#     except Exception as e:
#         print(f"Error inesperado: {e}")
#         return None


def processData_toCSV ():
    # Usamos el Singleton para obtener la conexión
    connection = DatabaseConnection().get_connection()

    # Si la conexión falló, salimos de la función
    if connection is None:
        print("No se pudo establecer la conexión a la base de datos a través del Singleton.")
        return
    try:
        # Analisis 1: Evolucion del salario por comunidades

        # LIMPIEZA Y ESTRUCTURACIÓN
        # Extraemos Salarios
        query_salarios = """
            SELECT s.valor as salario, g.nombre as comunidad, p.anio
            FROM T_salarios s
            JOIN tbl_geografia g ON s.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON s.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON s.id_indicador = i.id_indicador
            WHERE i.nombre LIKE 'Salario Anual Media'
        """

        df_salarios = pl.read_database(query=query_salarios, connection=connection) # Cargar en Polars

        # Procesamiento de salarios por comunidad 
        df_salarios_comunidades = (
            df_salarios.filter(pl.col("salario").is_not_null())
            .group_by(["comunidad", "anio"])
            .agg(pl.col("salario").mean().alias("salario_medio"))
            .sort(["comunidad", "anio"])
        ) # Se han eliminado los nulos y se ha calculado la media anual

        # Analisis 2: Ratio del poder adquisitivo

        # Extraemos IPC
        # filtramos por Total Nacional para el ipc para que sea el indice de referencia
        query_ipc = '''
            SELECT p.valor as ipc_valor, per.anio
            FROM T_precios p
            JOIN tbl_periodo per ON p.id_periodo = per.id_periodo
            JOIN tbl_geografia g ON p.id_geografia = g.id_geografia
            JOIN tbl_indicador i ON p.id_indicador = i.id_indicador
            WHERE i.nombre = 'IPC_Indice_Base_2021_INE' AND g.nombre = 'Total Nacional'
        '''
        df_ipc = pl.read_database(query=query_ipc, connection=connection)

        # Limpieza y join con polars
        # Agrupamos ipc por año para tener un valor anual
        df_ipc_anual = df_ipc.group_by('anio').agg(pl.col('ipc_valor').mean())

        # Unimos salarios con ipc por año para calcular el ratio
        df_poder_adquisitivo= df_salarios.join(df_ipc_anual, on='anio')


        # Columna calculada: calculo de poder adquisitivo
        # ratio = salario / ipc
        df_poder_adquisitivo = df_poder_adquisitivo.with_columns((pl.col('salario') / pl.col('ipc_valor')).alias('poder_adquisitivo'))

        # EXPORTACIÓN de los archivos (paso 3)
        # Definimos la ruta de salida hacia data_output
        output_dir = os.path.join(project_root, "data_output")

        # Verificamos que la carpeta existe
        os.makedirs(output_dir, exist_ok=True)

        # Exportamos los DataFrame procesados
        # CSV 1
        df_salarios_comunidades.write_csv(os.path.join(output_dir, "Evolucion_Salario_Comunidades.csv"))
        
        # CSV 2
        df_poder_adquisitivo.write_csv(os.path.join(output_dir, "Relacion_Poder_Adquisitivo.csv"))

        print(f"\nAnálisis finalizado. Se han generado 2 archivos en: {output_dir}")

    except Exception as e:
        print(f"Error en el procesamiento: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    processData_toCSV()
