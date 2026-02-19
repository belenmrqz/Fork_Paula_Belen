# transform.py
# ETL Fase 2: Transformación, limpieza y estructuración con Polars

import polars as pl
import os
import sys


# Aseguramos que Python encuentre la carpeta 'src' subiendo un nivel
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# Importamos la conexión original
from src.db import DatabaseConnection


def process_data_polars():
    """
    Función principal de transformación de datos.
    Extrae datos en bruto de SQLite, aplica lógica de negocio con Polars
    y genera los datasets finales en CSV y Parquet.
    """
    print("\nIniciando transformación de datos con Polars.")

    # Usamos el Singleton para obtener la conexión
    db_conn = DatabaseConnection().get_connection()

    # Si la conexión falló, salimos de la función
    if db_conn is None:
        print("Error: No se pudo establecer la conexión a la base de datos.")
        return

    try:
        # =======================================================================
        # FASE A: EXTRACCIÓN DE DATOS (SQL)
        # En lugar de múltiples queries, extraemos 3 grandes bloques de datos
        # para delegar todo el trabajo de filtrado a Polars.
        # =======================================================================
        print("Extrayendo datos maestros de la base de datos.")

        # 1. Bloque de Salarios
        query_all_salaries = """
            SELECT s.valor as salario, g.nombre as comunidad, p.anio, 
                   i.nombre as indicador, s.sexo, s.ocupacion_cno11 as ocupacion
            FROM T_salarios s
            JOIN tbl_geografia g ON s.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON s.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON s.id_indicador = i.id_indicador
        """
        df_master_salaries = pl.read_database(
            query=query_all_salaries, connection=db_conn
        )

        # 2. Bloque de Precios (IPC e IPV)
        query_all_prices = """
            SELECT pr.valor as precio, g.nombre as comunidad, p.anio, 
                   i.nombre as indicador, pr.categoria_gasto
            FROM T_precios pr
            JOIN tbl_geografia g ON pr.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON pr.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON pr.id_indicador = i.id_indicador
        """
        df_master_prices = pl.read_database(query=query_all_prices, connection=db_conn)

        # 3. Bloque de Empleo
        query_all_employment = """
            SELECT e.valor as valor_empleo, g.nombre as comunidad, p.anio, 
                   i.nombre as indicador, e.sexo, e.grupo_edad
            FROM T_empleo e
            JOIN tbl_geografia g ON e.id_geografia = g.id_geografia
            JOIN tbl_periodo p ON e.id_periodo = p.id_periodo
            JOIN tbl_indicador i ON e.id_indicador = i.id_indicador
        """
        df_master_employment = pl.read_database(
            query=query_all_employment, connection=db_conn
        )

        # =======================================================================
        # FASE B: TRANSFORMACIÓN Y ANÁLISIS EN MEMORIA (POLARS)
        # =======================================================================

        # -------------------------------------------------------------------
        # ANÁLISIS 1: Evolución del salario por comunidades
        # -------------------------------------------------------------------
        print("1/8 Procesando evolución salarial por CCAA.")

        df_salaries_regions = (
            # Filtramos para quedarnos solo con el salario medio anual y eliminar filas sin valor
            df_master_salaries.filter(
                (pl.col("indicador") == "Salario_Anual_Media")
                & (pl.col("salario").is_not_null())
            )
            # Agrupamos los datos por comunidad autónoma y año
            .group_by(["comunidad", "anio"])
            # Calculamos el promedio del salario para cada grupo creado
            .agg(pl.col("salario").mean().alias("salario_medio"))
            # Ordenamos los resultados por nombre de comunidad y año cronológico
            .sort(["comunidad", "anio"])
        )

        # -------------------------------------------------------------------
        # ANÁLISIS 2: Ratio de Poder Adquisitivo (Salario / IPC)
        # -------------------------------------------------------------------
        print("2/8 Calculando ratio de poder adquisitivo.")

        df_annual_cpi = (
            df_master_prices
            # Filtramos el índice del IPC nacional y general
            .filter(
                (pl.col("indicador") == "IPC_Indice")
                & (pl.col("comunidad") == "Total Nacional")
                & (pl.col("categoria_gasto") == "IPC General")
                & (pl.col("precio").is_not_null())
            )
            # Agrupamos por año para calcular la media de los 12 meses
            .group_by("anio")
            # Promediamos el valor del IPC para obtener un índice anual único
            .agg(pl.col("precio").mean().alias("ipc_valor"))
        )

        # Unimos (Join) la tabla de salarios regionales con la del IPC anual nacional
        df_purchasing_power = df_salaries_regions.join(df_annual_cpi, on="anio")

        # Creamos una columna nueva calculando el ratio (Salario dividido por el IPC)
        df_purchasing_power = df_purchasing_power.with_columns(
            (pl.col("salario_medio") / pl.col("ipc_valor")).alias("poder_adquisitivo")
        )

        # -------------------------------------------------------------------
        # ANÁLISIS 3: Comparativa IPV y Salarios
        # -------------------------------------------------------------------
        print("3/8 Comparando el precio de la vivienda y los salarios.")
        df_annual_hpi = (
            df_master_prices
            # Filtramos el indicador de precios de vivienda nacional
            .filter(
                (pl.col("indicador") == "IPV_Indice")
                & (pl.col("comunidad") == "Total Nacional")
                & (pl.col("precio").is_not_null())
            )
            # Agrupamos por año para promediar los datos trimestrales
            .group_by("anio")
            # Calculamos el valor promedio anual de la vivienda
            .agg(pl.col("precio").mean().alias("ipv"))
        )

        # Filtramos los salarios a nivel nacional para la comparativa
        df_total_salary = df_salaries_regions.filter(
            pl.col("comunidad") == "Total Nacional"
        ).select(["anio", "salario_medio"])

        # Unimos ambas tablas por año y ordenamos
        df_hpi_salary_comparison = df_total_salary.join(df_annual_hpi, on="anio").sort(
            "anio"
        )

        # Extraemos el valor del salario de 2015 para usarlo como base de normalización
        salary_2015 = (
            df_hpi_salary_comparison.filter(pl.col("anio") == 2015)
            .select("salario_medio")
            .item()
        )
        # Creamos el índice del salario base 100 para comparar el ritmo de subida con la vivienda
        df_hpi_salary_comparison = df_hpi_salary_comparison.with_columns(
            [(pl.col("salario_medio") / salary_2015 * 100).alias("indice_salario")]
        )

        # -------------------------------------------------------------------
        # ANÁLISIS 4: Brecha Salarial por Ocupación
        # -------------------------------------------------------------------
        print("4/8 Comparando el precio de la vivienda y los salarios.")

        df_gap_pivot = (
            df_master_salaries
            # Filtramos salarios por ocupación, excluyendo totales y registros incompletos
            .filter(
                (pl.col("indicador") == "Salario_Anual_Ocupacion")
                & (pl.col("sexo").is_in(["Hombres", "Mujeres"]))
                & (~pl.col("ocupacion").is_in(["Total", "N/A"]))
                & (pl.col("salario").is_not_null())
            )
            # Transformamos la tabla (pivot) para que 'Hombres' y 'Mujeres' sean columnas independientes
            .pivot(
                index=["anio", "ocupacion"],
                on="sexo",
                values="salario",
                aggregate_function="mean",
            )
            # Eliminamos cualquier fila que no tenga datos para ambos sexos
            .drop_nulls()
        )

        # Calculamos el porcentaje de brecha y limpiamos las anomalías del INE (valores negativos)
        df_gap_pivot = df_gap_pivot.with_columns(
            (((pl.col("Hombres") - pl.col("Mujeres")) / pl.col("Hombres")) * 100).alias(
                "brecha_porcentual"
            )
        ).filter(
            (pl.col("brecha_porcentual").is_between(-100, 100))
            & (pl.col("Mujeres") > 0)
            & (pl.col("Hombres") > 0)
        )

        # -------------------------------------------------------------------
        # ANÁLISIS 5: Curva Salarial (Paro y salarios) y Correlación
        # -------------------------------------------------------------------
        print("5/8 Calculando correlaciones entre tasa de paro y salarios.")

        df_annual_unemployment = (
            df_master_employment
            # Filtramos la tasa de paro por comunidad para la población general
            .filter(
                (pl.col("indicador") == "Tasa_Paro")
                & (pl.col("comunidad") != "Total Nacional")
                & (pl.col("sexo") == "Ambos sexos")
                & (pl.col("grupo_edad") == "Todas las edades")
                & (pl.col("valor_empleo").is_not_null())
            )
            # Agrupamos por comunidad y año para convertir datos trimestrales a anuales
            .group_by(["comunidad", "anio"])
            # Calculamos la tasa de paro media anual
            .agg(pl.col("valor_empleo").mean().alias("tasa_paro_media"))
        )

        # Unimos los datos de salarios y paro por comunidad y año
        df_unemployment_salaries = (
            df_salaries_regions.filter(pl.col("comunidad") != "Total Nacional")
            .join(df_annual_unemployment, on=["comunidad", "anio"])
            .sort(["anio", "comunidad"])
        )

        # Calculamos el coeficiente de correlación de Pearson entre ambas variables por cada región
        df_correlation = (
            df_unemployment_salaries.group_by("comunidad")
            .agg(
                pl.corr("tasa_paro_media", "salario_medio").alias("correlacion_pearson")
            )
            .sort("correlacion_pearson")
        )

        # ----------------------------------------------------------------------------------
        # ANÁLISIS 6: Comparativa Salario Nominal VS Salario Real (impacto de la inflación)
        # ----------------------------------------------------------------------------------
        print("6/8 Deflactando inflación (Nominal vs Real).")
        df_annual_nominal_salary = (
            df_master_salaries
            # Seleccionamos el coste salarial trimestral nacional
            .filter(
                (pl.col("indicador") == "Salario_Coste_Trimestral")
                & (pl.col("comunidad") == "Total Nacional")
                & (pl.col("salario").is_not_null())
            )
            # Agrupamos por año para obtener la media nominal
            .group_by("anio")
            # Calculamos el salario medio nominal (sin ajustar por inflación)
            .agg(pl.col("salario").mean().alias("salario_nominal"))
        )

        # Unimos con el IPC anual y creamos la columna del Salario Real (Deflactado)
        df_real_salary_comparison = (
            df_annual_nominal_salary.join(df_annual_cpi, on="anio")
            .sort("anio")
            .with_columns(
                ((pl.col("salario_nominal") / pl.col("ipc_valor")) * 100).alias(
                    "salario_real"
                )
            )
        )

        # ----------------------------------------------------------------------------------
        # ANÁLISIS 7: Calidad Empleo (Contrato Temporal vs Indefinido)
        # ----------------------------------------------------------------------------------

        print("7/8 Evaluando calidad del empleo (Temporalidad).")
        df_job_quality = (
            df_master_employment
            # Filtramos los datos de asalariados totales y temporales
            .filter(
                (
                    pl.col("indicador").is_in(
                        ["Asalariados_Total", "Asalariados_Temporal"]
                    )
                )
                & (pl.col("comunidad") == "Total Nacional")
                & (pl.col("valor_empleo").is_not_null())
            )
            # Agrupamos por año e indicador para obtener medias anuales limpias
            .group_by(["anio", "indicador"])
            .agg(pl.col("valor_empleo").mean())
            # Convertimos los indicadores a columnas para operar entre ellas
            .pivot(values="valor_empleo", index="anio", on="indicador")
            .sort("anio")
            # Calculamos los porcentajes de empleo temporal e indefinido
            .with_columns(
                (
                    (pl.col("Asalariados_Temporal") / pl.col("Asalariados_Total")) * 100
                ).alias("Temporal (%)"),
                (
                    (
                        (pl.col("Asalariados_Total") - pl.col("Asalariados_Temporal"))
                        / pl.col("Asalariados_Total")
                    )
                    * 100
                ).alias("Indefinido (%)"),
            )
        )

        # ----------------------------------------------------------------------------------
        # ANÁLISIS 8: Desigualdad Salarial
        # ----------------------------------------------------------------------------------
        print("8/8 Procesando tramos de ingresos y desigualdad salarial.")
        percentiles = [
            "Salario_Anual_Media",
            "Salario_Anual_Mediana",
            "Salario_Anual_Percentil 10",
            "Salario_Anual_Cuartil inferior",
        ]

        df_annual_percentiles = (
            df_master_salaries
            # Filtramos todos los estadísticos de interés a nivel nacional
            .filter(
                (pl.col("indicador").is_in(percentiles))
                & (pl.col("comunidad") == "Total Nacional")
                & (pl.col("sexo") == "Total")
                & (pl.col("salario").is_not_null())
            )
            # Agrupamos para obtener el valor anual por cada tipo de estadístico
            .group_by(["anio", "indicador"])
            .agg(pl.col("salario").mean())
            .sort(["anio", "salario"])
        )

        # =======================================================================
        # FASE C: EXPORTACIÓN A CSV Y PARQUET
        # =======================================================================
        print("\nExportando resultados a las carpetas CSV y Parquet.")
        # Definimos la ruta de salida hacia data_output
        output_dir = os.path.join(project_root, "data_output")

        # Verificamos que las carpetas existen
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, "csv"), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "parquet"), exist_ok=True)

        # Diccionario con los nombres de los archivos y sus dataframes
        datasets = {
            "Evolucion_Salario_Comunidades": df_salaries_regions,
            "Relacion_Poder_Adquisitivo": df_purchasing_power,
            "Comparativa_Vivienda_Salario": df_hpi_salary_comparison,
            "Brecha_Salarial_Ocupacion": df_gap_pivot,
            "Relacion_Paro_Salarios": df_unemployment_salaries,
            "Correlacion_Paro_Salarios": df_correlation,
            "Salario_Nominal_vs_Real": df_real_salary_comparison,
            "Calidad_Empleo": df_job_quality,
            "Desigualdad_Salarial": df_annual_percentiles,
        }

        for file_name, df in datasets.items():
            # Exportar como CSV
            df.write_csv(os.path.join(output_dir, "csv", f"{file_name}.csv"))
            # Exportar como Parquet
            df.write_parquet(
                os.path.join(output_dir, "parquet", f"{file_name}.parquet")
            )

        print(
            f"\nFase ETL finalizada. {len(datasets)} datasets generados en '{output_dir}'"
        )

    except Exception as e:
        print(f"Error en el procesamiento: {e}")
    finally:
        db_conn.close()


if __name__ == "__main__":
    process_data_polars()
