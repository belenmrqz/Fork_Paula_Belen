"""
Recibe los datos de inedata.py y los prepara para la base de datos
"""

def obtener_o_crear_periodo(conn, anio, mes, trimestre=None):
    """
    Busca si un periodo existe. Si existe, devuelve su ID.
    Si no existe, lo crea y devuelve el nuevo ID.
    """
    cursor = conn.cursor()
    
    # 1. Crear la clave única de búsqueda (ej: 2023-01-01 o 2023-T4)
    # Aquí es donde se estandariza el formato TEXT para la clave única.
    if mes is not None:
        fecha_iso = f"{anio}-{str(mes).zfill(2)}-01"  # Mensual
    elif trimestre is not None:
        fecha_iso = f"{anio}-T{trimestre}-01"         # Trimestral
    else:
        fecha_iso = f"{anio}-01-01"                   # Anual

    # 2. Intentar buscar el ID (Optimización 1: Evitar el INSERT si ya existe)
    cursor.execute("SELECT id_periodo FROM tbl_periodo WHERE fecha_iso = ?", (fecha_iso,))
    resultado = cursor.fetchone()
    
    if resultado:
        # El periodo ya existe. Devolvemos su ID.
        return resultado[0]
    else:
        # 3. El periodo no existe. Lo insertamos.
        cursor.execute("""
            INSERT INTO tbl_periodo (anio, mes, trimestre, fecha_iso) 
            VALUES (?, ?, ?, ?)
        """, (anio, mes, trimestre, fecha_iso))
        
        # 4. Devolvemos el ID de la nueva fila insertada
        return cursor.lastrowid

# --- Uso dentro del proceso de ETL ---

# # Ejemplo: Procesando un dato de la EPA (Trimestral)
# anio_dato = 2023
# trimestre_dato = 4

# # 1. Conexión y obtención del ID
# conn = sqlite3.connect(DB_NAME)
# id_periodo = obtener_o_crear_periodo(conn, anio_dato, None, trimestre_dato) # mes es None
# conn.commit()

# # 2. Usar ese ID para insertar en la tabla de hechos T_empleo
# tasa_paro_valor = 12.5 
# cursor.execute("""
#     INSERT INTO T_empleo (id_periodo, id_indicador, sexo, valor) 
#     VALUES (?, ?, ?, ?)
# """, (id_periodo, 1, "Ambos", tasa_paro_valor)) # id_indicador 1 = Tasa Paro

# conn.commit()
# conn.close()