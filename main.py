from config.constantes import IPC,  IPV, TASA_PARO, TEMPORALIDAD, EAES_OCUPACION, EAES_PERCENTILES, ETCL
from src.inedata import INEDataExtractor
from src.procesar import procesar_datos
from src.almacenar import insertar_datos
from src.db import DatabaseConnection, crear_base_datos


def main():
   
   db = DatabaseConnection.get_connection()
   crear_base_datos()

   tablas = [IPC, IPV, TASA_PARO, TEMPORALIDAD, EAES_OCUPACION, EAES_PERCENTILES, ETCL]
   for codigo in tablas:
        extractor = INEDataExtractor(codigo)
        if extractor.obtener_datos():
           datos_procesados = procesar_datos(db, codigo, extractor.raw_data)
           insertar_datos(db, datos_procesados)
        else:
            print(f"No se pudieron obtener los datos de la tabla {codigo}")


   DatabaseConnection.close()

if __name__ == "__main__":
    main()
