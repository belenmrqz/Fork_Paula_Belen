# transform.py
# Paso 1 y 2: Conexión y limpieza con Polars

import polars as pl
import sqlite3
import os 

# 1. Obtenemos la ruta absoluta de la carpeta donde está ESTE archivo (analysis)
current_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Subimos un nivel para llegar a la raíz del proyecto
project_root = os.path.dirname(current_dir)

# 3. Construimos la ruta a la base de datos de forma absoluta
db_path = os.path.join(project_root, "proyecto_datos.db")
# Ruta a la base de datos


# método para establecer la conexión con la BBDD
def establish_connection ():
    try:
        normalized_path = os.path.abspath(db_path) # Normalizamos la ruta para evitar errores visuales

        if not os.path.exists(db_path): # Comprobar que existe
            raise FileExistsError(f"No se encuentra el archivo {db_path}")
        
        connection = sqlite3.connect(db_path) # Establecer conexión

        query_test = "SELECT name FROM sqlite_master WHERE type='table';" # Prueba
        tables = connection.execute(query_test).fetchall()


        print("\nCONEXIÓN EXITOSA A LA BASE DE DATOS")
        print(f"Ubicación -> {normalized_path}")
        print("Tablas encontradas: ")
        for table in tables:
            print(f"   - {table[0]}")
        
        return connection


    # Posibles errores:
    except FileNotFoundError as e:
        print(f"Error de ruta: {e}")
        return None
    except sqlite3.Error as e:
        print(f"Error de SQLite al conectar: {e}")
        return None
    except Exception as e:
        print(f"Error inesperado: {e}")
        return None



# Método de carga en Polars
def data_load ():
    connection = establish_connection() # Establecer conexión

    if connection:
        try:
            # Extraemos datos cruzados como sugiere el diseño previo Star Schema 
            query = """
            SELECT s.valor, g.nombre as comunidad, p.anio, i.nombre as indicador
            FROM T_salarios s
            JOIN tbl_geografia g ON s.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON s.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON s.id_indicador = i.id_indicador
            LIMIT 10
            """
            df = pl.read_database(query=query, connection=connection)
            print("Carga de datos en Polars exitosa:")
            print(df)
        except Exception as e:
            print(f"Error al cargar datos en Polars: {e}")
        finally:
            connection.close()

# if __name__ == "__main__":
#     data_load()


def processData_toCSV ():

    connection = establish_connection() # Establecer conexión 

    # Si la conexión falló, salimos de la función
    if connection is None:
        return
    try:

        # LIMPIEZA Y ESTRUCTURACIÓN 
        # Extraemos Salarios e IPC cruzados con sus dimensiones 
        query_salarios = '''
            SELECT s.valor, g.nombre as comunidad, p.anio, i.nombre as indicador
            FROM T_salarios s
            JOIN tbl_geografia g ON s.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON s.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON s.id_indicador = i.id_indicador
            WHERE i.nombre LIKE '%Ganancia%'
        '''

        df_salarios = pl.read_database(query=query_salarios, connection=connection) # Cargar en Polars

        df_salarios_clean = ( # Limpieza con Polars
            df_salarios.filter(pl.col("valor").is_not_null()).group_by(["comunidad", "anio"]).agg(pl.col("valor").mean().alias("salario_medio")).sort(["comunidad", "anio"])
        ) #Se han eliminado los nulos y se ha calculado la media anual




        # GENERACIÓN DE DATASET PARA INFORMES
        # Definimos la ruta de salida hacia data_output 
        output_path = os.path.join(project_root, "data_output", "Evolucion_Salario_Comunidades.csv")
        
        # Verificamos que la carpeta existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Exportamos el DataFrame procesado 
        df_salarios_clean.write_csv(output_path)
        
        
        print(f"\nArchivo generado en: {output_path}")
        print(df_salarios_clean.head())

    except Exception as e:
        print(f"Error en el procesamiento: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    processData_toCSV()
    