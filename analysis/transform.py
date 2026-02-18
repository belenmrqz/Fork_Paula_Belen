# transform.py
# Paso 1 y 2: Conexión y limpieza con Polars

import polars as pl
import sqlite3
import os
import sys


# 1. Aseguramos que Python encuentre la carpeta 'src' subiendo un nivel
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# 2. Importamos vuestra conexión original
from src.db import DatabaseConnection


def processData_toCSV():
    # Usamos el Singleton para obtener la conexión
    connection = DatabaseConnection().get_connection()

    # Si la conexión falló, salimos de la función
    if connection is None:
        print(
            "No se pudo establecer la conexión a la base de datos a través del Singleton."
        )
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
            WHERE i.nombre LIKE 'Salario_Anual_Media'
        """

        df_salarios = pl.read_database(
            query=query_salarios, connection=connection
        )  # Cargar en Polars

        # Procesamiento de salarios por comunidad
        df_salarios_comunidades = (
            df_salarios.filter(pl.col("salario").is_not_null())
            .group_by(["comunidad", "anio"])
            .agg(pl.col("salario").mean().alias("salario_medio"))
            .sort(["comunidad", "anio"])
        )  # Se han eliminado los nulos y se ha calculado la media anual

        # Analisis 2: Ratio del poder adquisitivo

        # Extraemos IPC
        # filtramos por Total Nacional para el ipc para que sea el indice de referencia
        query_ipc = """
            SELECT p.valor as ipc_valor, per.anio
            FROM T_precios p
            JOIN tbl_periodo per ON p.id_periodo = per.id_periodo
            JOIN tbl_geografia g ON p.id_geografia = g.id_geografia
            JOIN tbl_indicador i ON p.id_indicador = i.id_indicador
            WHERE i.nombre = 'IPC_Indice' AND g.nombre = 'Total Nacional'
        """
        df_ipc = pl.read_database(query=query_ipc, connection=connection)

        # Analisis 3: Carrera entre salarios y vivienda
        # ¿los salarios han crecido al mismo ritmo que le precio de la vivienda?
        query_ipv = """ 
            SELECT p.valor as ipv, per.anio
            FROM T_precios p
            JOIN tbl_periodo per ON p.id_periodo = per.id_periodo
            JOIN tbl_indicador i on p.id_indicador = i.id_indicador
            JOIN tbl_geografia g ON p.id_geografia = g.id_geografia
            WHERE i.nombre =  'IPV_Indice'
            AND   g.nombre = 'Total Nacional'

        """
        df_ipv = pl.read_database(query=query_ipv, connection=connection)
        df_ipv = (df_ipv.group_by('anio').agg(pl.col("ipv").mean()))
        df_salario_total = (
            df_salarios.filter(pl.col("comunidad") == "Total Nacional")
            .group_by("anio")
            .agg(pl.col("salario").mean())
        )

        df_comparativa_ipv_salario = df_salario_total.join(df_ipv, on="anio").sort(
            "anio"
        )

        # Normalizamos a base 100 usando 2015 como es el caso del ipv
        salario_2015 = df_comparativa_ipv_salario.filter(pl.col("anio") == 2015).select("salario").item()
        df_comparativa_ipv_salario = df_comparativa_ipv_salario.with_columns(
            [
                (pl.col("salario") / salario_2015 * 100).alias(
                    "indice_salario"
                )
            ]
        )
        # Limpieza y join con polars
        # Agrupamos ipc por año para tener un valor anual
        df_ipc_anual = df_ipc.group_by("anio").agg(pl.col("ipc_valor").mean())

        # Unimos salarios con ipc por año para calcular el ratio
        df_poder_adquisitivo = df_salarios.join(df_ipc_anual, on="anio")

        # Columna calculada: calculo de poder adquisitivo
        # ratio = salario / ipc
        df_poder_adquisitivo = df_poder_adquisitivo.with_columns(
            (pl.col("salario") / pl.col("ipc_valor")).alias("poder_adquisitivo")
        )


        # Análisis 4: BRECHA SALARIAL DE GÉNERO POR OCUPACIÓN
        
        # 1. Extraemos solo los salarios anuales por ocupación, filtrando hombres y mujeres (quitamos el 'Total')
        query_brecha = """
            SELECT s.valor as salario, s.sexo, s.ocupacion_cno11 as ocupacion, p.anio
            FROM T_salarios s
            JOIN tbl_periodo p ON s.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON s.id_indicador = i.id_indicador
            WHERE i.nombre = 'Salario_Anual_Ocupacion' 
              AND s.sexo IN ('Hombres', 'Mujeres')
              AND s.ocupacion_cno11 NOT IN ('Total', 'N/A')
              AND s.valor IS NOT NULL
        """
        df_brecha = pl.read_database(query=query_brecha, connection=connection)

        # 2. Hacemos un PIVOT: Queremos que 'Hombres' y 'Mujeres' sean columnas para poder restarlas
        # Cuidado con la sintaxis de pivot en Polars, es súper potente
        df_brecha_pivot = df_brecha.pivot(
            index=["anio", "ocupacion"],  # Lo que se queda como filas fijas
            on="sexo",               # Lo que se va a desdoblar en columnas
            values="salario",             # Los valores que van dentro
            aggregate_function="mean"     # Por si hay algún duplicado (media)
        )

        # 3. Limpiamos por si alguna ocupación solo tiene datos de un sexo (evita nulos)
        df_brecha_pivot = df_brecha_pivot.drop_nulls()

        # 4. Calculamos la brecha porcentual: ((Hombres - Mujeres) / Hombres) * 100
        df_brecha_pivot = df_brecha_pivot.with_columns(
            (((pl.col("Hombres") - pl.col("Mujeres")) / pl.col("Hombres")) * 100).alias("brecha_porcentual")
        )

        # 5.Filtramos donde la brecha sea mayor al 100%, o donde algún salario sea negativo o cero
        df_brecha_pivot = df_brecha_pivot.filter(
            (pl.col("brecha_porcentual") <= 100) & 
            (pl.col("brecha_porcentual") >= -100) &
            (pl.col("Mujeres") > 0) & 
            (pl.col("Hombres") > 0)
        )

        # Analisis 4: Curva salarial (Paro vs Salarios por CCAA)
        # 1. Extraemos Tasa de Paro (Media anual por CCAA)
        # Filtramos 'Ambos sexos' y 'Todas las edades' para no duplicar datos
        query_paro = """
            SELECT e.valor as tasa_paro, g.nombre as comunidad, p.anio
            FROM T_empleo e
            JOIN tbl_geografia g ON e.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON e.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON e.id_indicador = i.id_indicador
            WHERE i.nombre = 'Tasa_Paro' 
              AND g.nombre != 'Total Nacional'
              AND e.sexo = 'Ambos sexos'
              AND e.grupo_edad = 'Todas las edades'
        """
        df_paro = pl.read_database(query=query_paro, connection=connection)
        
        # Como el paro en el INE es trimestral, calculamos la media anual por comunidad
        df_paro_anual = (
            df_paro.filter(pl.col("tasa_paro").is_not_null())
            .group_by(["comunidad", "anio"])
            .agg(pl.col("tasa_paro").mean().alias("tasa_paro_media"))
        )

        # 2. Extraemos Salarios por CCAA (quitando Total Nacional)
        df_salarios_anual = (
            df_salarios.filter((pl.col("salario").is_not_null()) & (pl.col('comunidad') != 'Total Nacional'))
            .group_by(["comunidad", "anio"])
            .agg(pl.col("salario").mean().alias("salario_medio"))
        )

        # 3. Unimos (Inner Join) las dos tablas en base a Comunidad y Año
        df_paro_salarios = df_salarios_anual.join(df_paro_anual, on=["comunidad", "anio"])

        # ORDENAR para que la animacion en plotly sea correcta por años
        df_paro_salarios = df_paro_salarios.sort(["anio", "comunidad"])

        # EXTRA: CÁLCULO DE CORRELACIÓN DE PEARSON POR CCAA ---
        # Calculamos la correlación matemática exacta entre el paro y los salarios
        df_correlacion = (
            df_paro_salarios.group_by("comunidad")
            .agg(pl.corr("tasa_paro_media", "salario_medio").alias("correlacion_pearson"))
            .sort("correlacion_pearson") # Ordenamos de la más negativa a la más positiva
        )


        # Analisis 5: Salario nominal vs salario real
        
        query_salario_nom = """
            SELECT s.valor as salario_nominal, p.anio
            FROM T_salarios s
            JOIN tbl_geografia g ON s.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON s.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON s.id_indicador = i.id_indicador
            WHERE i.nombre = 'Salario_Coste_Trimestral'
              AND g.nombre = 'Total Nacional'
        """
        df_salario_nom = pl.read_database(query=query_salario_nom, connection=connection)
        
        # Como es trimestral/mensual, calculamos la media que se cobró cada año
        df_salario_nom_anual = (
            df_salario_nom.filter(pl.col("salario_nominal").is_not_null())
            .group_by("anio")
            .agg(pl.col("salario_nominal").mean())
        )

        # Extraemos el ipc nacional
        query_ipc_nal = """
            SELECT pr.valor as ipc, p.anio
            FROM T_precios pr
            JOIN tbl_geografia g ON pr.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON pr.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON pr.id_indicador = i.id_indicador
            WHERE i.nombre = 'IPC_Indice'
            AND g.nombre = 'Total Nacional'
            AND pr.categoria_gasto = 'IPC General'
        """
        df_ipc_nal = pl.read_database(query=query_ipc_nal, connection=connection)

        # Agrupamos por año (media de los meses)
        df_ipc_anual = (
            df_ipc_nal.filter(pl.col("ipc").is_not_null())
            .group_by("anio")
            .agg(pl.col("ipc").mean())
        )


        # Unimos las tablas y calculamos el Salario Real
        df_real = df_salario_nom_anual.join(df_ipc_anual, on="anio").sort("anio")

        # Aplicamos la fórmula de deflactación
        df_real = df_real.with_columns(
            ((pl.col("salario_nominal") / pl.col("ipc")) * 100).alias("salario_real")
        )

        # ANALISIS CALIDAD EMPLEO (TEMPORALIDAD VS INDEFINIDOS)
        query_empleo = """
            SELECT e.valor, p.anio, i.nombre as indicador
            FROM T_empleo e
            JOIN tbl_geografia g ON e.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON e.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON e.id_indicador = i.id_indicador
            WHERE i.nombre IN ('Asalariados_Total', 'Asalariados_Temporal')
              AND g.nombre = 'Total Nacional'
        """
        df_empleo = pl.read_database(query=query_empleo, connection=connection)
        
        # Hacemos la media anual por indicador
        df_empleo_anual = (
            df_empleo.filter(pl.col("valor").is_not_null())
            .group_by(["anio", "indicador"])
            .agg(pl.col("valor").mean())
        )

        # Pivotamos para tener los indicadores como columnas
        df_pivot_empleo = df_empleo_anual.pivot(
            values="valor",
            index="anio",
            on="indicador"
        ).sort("anio")

        # Calculamos los porcentajes (El indefinido es el Total menos el Temporal)
        df_calidad = df_pivot_empleo.with_columns(
            ((pl.col("Asalariados_Temporal") / pl.col("Asalariados_Total")) * 100).alias("Temporal (%)"),
            (((pl.col("Asalariados_Total") - pl.col("Asalariados_Temporal")) / pl.col("Asalariados_Total")) * 100).alias("Indefinido (%)")
        )

        # analisis: DESIGUALDAD SALARIAL (Media vs Mediana vs Percentiles bajos) ---
        
        query_percentiles = """
            SELECT s.valor as salario, p.anio, i.nombre as indicador
            FROM T_salarios s
            JOIN tbl_geografia g ON s.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON s.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON s.id_indicador = i.id_indicador
            WHERE i.nombre IN (
                'Salario_Anual_Media', 
                'Salario_Anual_Mediana', 
                'Salario_Anual_Percentil 10', 
                'Salario_Anual_Cuartil inferior'
            )
              AND g.nombre = 'Total Nacional'
              AND s.sexo = 'Total'
        """
        df_percentiles = pl.read_database(query=query_percentiles, connection=connection)

        # Calculamos la media por año e indicador para tener un punto limpio anual
        df_percentiles_anual = (
            df_percentiles.filter(pl.col("salario").is_not_null())
            .group_by(["anio", "indicador"])
            .agg(pl.col("salario").mean())
            .sort(["anio", "salario"]) # Ordenamos para que Plotly lea bien la línea de tiempo
        )

        # --- EXPORTACIÓN de los archivos (paso 3) ---
        # Definimos la ruta de salida hacia data_output
        output_dir = os.path.join(project_root, "data_output")

        # Verificamos que la carpeta existe
        os.makedirs(output_dir, exist_ok=True)

        # Exportamos los DataFrame procesados
        # CSV 1
        df_salarios_comunidades.write_csv(
            os.path.join(output_dir, "Evolucion_Salario_Comunidades.csv")
        )

        # CSV 2
        df_poder_adquisitivo.write_csv(
            os.path.join(output_dir, "Relacion_Poder_Adquisitivo.csv")
        )

        # CSV 3
        df_comparativa_ipv_salario.write_csv(
            os.path.join(output_dir, "Comparativa_Vivienda_Salario.csv")
        )

        # CSV 4
        df_brecha_pivot.write_csv(os.path.join(output_dir, "Brecha_Salarial_Ocupacion.csv"))

        # CSV 5
        df_paro_salarios.write_csv(os.path.join(output_dir, "Relacion_Paro_Salarios.csv"))
        df_correlacion.write_csv(os.path.join(output_dir, "Correlacion_Paro_Salarios.csv"))

        # CSV 6
        df_real.write_csv(os.path.join(output_dir, "Salario_Nominal_vs_Real.csv"))

        # CSV 7
        df_calidad.write_csv(os.path.join(output_dir, "Calidad_Empleo.csv"))

        # CSV 8
        df_percentiles_anual.write_csv(os.path.join(output_dir, "Desigualdad_Salarial.csv"))

        print(f"\nAnálisis finalizado. Se han generado los archivos en: {output_dir}")

    except Exception as e:
        print(f"Error en el procesamiento: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    processData_toCSV()
