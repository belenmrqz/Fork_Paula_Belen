from config.constantes import IPC, IPV, ETCL, EAES_OCUPACION, EAES_PERCENTILES, TASA_PARO, TEMPORALIDAD_JORNADA
from src.db import get_cursor

def procesar_datos(codigo, datos):
    """
    Función principal de transformación. Despacha el procesamiento 
    a las funciones especializadas según el código INE.
    """
    # 1. SEGREGACIÓN POR GRUPO DE TABLA DE HECHOS
    
    if codigo in [IPC, IPV]:
        # T1: Precios (Mensual y Trimestral)
        return _procesar_precios(codigo, datos)
        
    elif codigo in [ETCL, EAES_OCUPACION, EAES_PERCENTILES]:
        # T2: Ingresos/Salarios (Trimestral y Anual)
        return _procesar_salarios(codigo, datos)
        
    elif codigo in [TASA_PARO, TEMPORALIDAD_JORNADA]:
        # T3: Empleo/Calidad Laboral (Trimestral)
        return _procesar_empleo(codigo, datos)
        
    else:
        print(f"[Procesar] ERROR: Código {codigo} no mapeado a una tabla de hechos.")
        return []

def _aplanar_nombre_serie(codigo, nombre_serie):
    """ Función que contiene la lógica IF/ELIF para parsear los Nombres. """
    
    metadata = {}
    nombre =  nombre_serie.split(".")

    if codigo == TASA_PARO:
        metadata["Sexo"] = nombre[1].strip()
        metadata["Grupo_Edad"] = nombre[3].strip()
        metadata["Geografia"] = nombre[2].strip()

    elif codigo == TEMPORALIDAD_JORNADA:
        metadata["Sexo"] = nombre[2].strip()
        metadata["Tipo_Jornada"] = nombre[4].strip()
        metadata["Tipo_Contrato"] = nombre[3].strip()
        metadata["Geografia"] = nombre[0].strip()
        metadata["Unidad"] = nombre[5].strip()



    return metadata
   




def _procesar_empleo(codigo, data):
    filas_insertar = []
    for serie in data:
        # 1. Obtener los metadatos del nombre
        nombre_serie = serie.get("Nombre", "N/A")
        metadata_dims = _aplanar_nombre_serie(codigo, nombre_serie)

        # Obtener IDs de los FK (indicador y geografia)
        id_geografia = _obtener_o_crear("tbl_geografia", "nombre", metadata_dims.get("Geografia"))
        
        nombre_indicador = ""
        unidad = ""

        if codigo == TASA_PARO:
            nombre_indicador = "Tasa_Paro"
            unidad = None

        elif codigo == TEMPORALIDAD_JORNADA:
            nombre_indicador = "Temporalidad_Ocupados"
            unidad = metadata_dims.get("Unidad")
        
        else: raise ValueError
        id_indicador = _obtener_o_crear("tbl_indicador", "nombre", nombre_indicador, unidad=unidad)

        for dato in serie.get("Data"):
            # Obtener ID del periodo
            id_periodo = _obtener_o_crear_periodo(anio=dato.get("Anyo"), trimestre_fk=dato.get("Fk_Periodo"))
            sexo = metadata_dims.get("Sexo")
            grupo_edad = metadata_dims.get("Grupo_Edad", None)
            tipo_jornada = metadata_dims.get("Tipo_Jornada", None)
            tipo_contrato = metadata_dims.get("Tipo_Contrato", None)
            valor = dato.get("Valor")

            filas_insertar.append([id_periodo, id_indicador, id_geografia, sexo, grupo_edad, tipo_jornada, tipo_contrato, valor])

        return filas_insertar







def _procesar_precios(codigo, data):
    pass
def _procesar_salarios(codigo, data):
    pass

def _obtener_o_crear_periodo(anio, mes= None, trimestre_fk=None):
    """
    Busca si un periodo existe. Si existe, devuelve su ID.
    Si no existe, lo crea y devuelve el nuevo ID.

    :param anio: Año (INTEGER).
    :param mes: Mes (1-12) (INTEGER) [Para datos mensuales].
    :param trimestre_fk: Código FK_Periodo del INE (19-22) [Para datos trimestrales].
    """
    
    # 1. Crear la clave única de búsqueda (ej: 2023-01-01)
    mes_calculado = mes
    if mes is not None:
        fecha_iso = f"{anio}-{str(mes).zfill(2)}-01" # Mensual

    elif trimestre_fk is not None:
        if trimestre_fk == 19: mes_calculado = 1
        elif trimestre_fk == 20: mes_calculado = 4 #Q2
        elif trimestre_fk == 21: mes_calculado = 7 #Q3
        elif trimestre_fk == 22: mes_calculado = 10 #Q4
        else:
            raise ValueError(f"Código de trimestre INE '{trimestre_fk}' no reconocido.")

        fecha_iso = f"{anio}-{str(mes_calculado).zfill(2)}-01" # Trimestral
    else:
        fecha_iso = f"{anio}-01-01" # Anual

    return _obtener_o_crear(
        tabla="periodo", 
        columna_busqueda="fecha_iso", 
        valor_busqueda=fecha_iso, 
        anio=anio, 
        trimestre=trimestre_fk, 
        mes=mes_calculado
    )

def _obtener_o_crear(tabla, columna_busqueda, valor_busqueda, **kwargs):
    """
    Función centralizada para buscar o crear una dimensión (Periodo, Geografía, Indicador).
    """
    id_columna = f"id_{tabla}"
    tabla_nombre = f"tbl_{tabla}"

    with get_cursor() as cursor:

        # 2. Intentar buscar el ID 
        sql_select = f"SELECT {id_columna} FROM {tabla_nombre} WHERE {columna_busqueda} = ?"
        cursor.execute(sql_select, (valor_busqueda,))
        resultado = cursor.fetchone()
        
        if resultado:
            # El periodo ya existe. Devolvemos su ID.
            return resultado[0]
        
        # 3. El periodo no existe. Lo insertamos.
        sql_insert = ""
        if tabla == "periodo":
            sql_insert = """
            INSERT INTO tbl_periodo (anio, mes, trimestre, fecha_iso) 
            VALUES (?, ?, ?, ?)
            """
            parametros = (
                kwargs.get("anio"), 
                kwargs.get("mes"), 
                kwargs.get("trimestre"), 
                valor_busqueda # fecha_iso
            )

        elif tabla == "geografia":
            sql_insert = """
            INSERT INTO tbl_geografia (nombre)
            VALUES (?)
            """
            parametros = (valor_busqueda,)
        
        elif tabla == "indicador":
            sql_insert = """
            INSERT INTO tbl_indicador (nombre, unidad)
            VALUES (?, ?)
            """
            parametros = (valor_busqueda, kwargs.get("unidad"),)

        else:
            raise ValueError(f"Tabla de dimensión '{tabla}' no soportada por _obtener_o_crear.")
        
        cursor.execute(sql_insert, parametros)

        # 4. Devolvemos el ID de la nueva fila insertada
        return cursor.lastrowid