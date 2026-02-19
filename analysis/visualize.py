# ETL Fase 3: Visualización con Plotly
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import os

# Configuración de rutas
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
data_dir = os.path.join(project_root, "data_output")
csv_dir = os.path.join(data_dir, "csv")
os.makedirs(os.path.join(data_dir, "graphics"), exist_ok=True)



def generate_plotly_charts():
    """
    Función principal de visualización.
    Lee los datasets de los CSVs y genera gráficos interactivos en HTML.
    """
    print("\nIniciando generación de gráficos con Plotly.")

    try:
        # -------------------------------------------------------------------
        # GRÁFICO 1: Evolución del salario por comunidad autónoma
        # -------------------------------------------------------------------
        print("1/8 Generando gráfico de evolución salarial.")
        df_salaries = pl.read_csv(
            os.path.join(csv_dir, "Evolucion_Salario_Comunidades.csv")
        )

        # Este gráfico permite ver cómo han crecido los salarios en cada sitio
        fig1 = px.line(
            df_salaries,
            x="anio",
            y="salario_medio",
            color="comunidad",
            title="Evolución del Salario Medio Anual por CCAA",
            markers=True,
            labels={"salario_medio": "Euros (€)", "anio": "Año"},
        )
        fig1.write_html(os.path.join(data_dir, "graphics", "1_evolucion_salarios.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 2: 'Carrera' de salarios y precio de la vivienda
        # -------------------------------------------------------------------
        print("2/8 Dibujando carrera Salarios vs Vivienda.")
        df_comparison = pl.read_csv(
            os.path.join(csv_dir, "Comparativa_Vivienda_Salario.csv")
        )

        fig2 = go.Figure()

        # Línea de Salarios
        fig2.add_trace(
            go.Scatter(
                x=df_comparison["anio"],
                y=df_comparison["indice_salario"],
                mode="lines+markers",
                name="Salarios (Base 100)",
            )
        )

        # Línea de Vivienda
        fig2.add_trace(
            go.Scatter(
                x=df_comparison["anio"],
                y=df_comparison["ipv"],
                mode="lines+markers",
                name="Precio Vivienda (Base 100)",
                line=dict(dash="dot"),  # Línea punteada para diferenciar
            )
        )

        fig2.update_layout(
            title="Carrera de Precios: Salarios vs Vivienda (Base 100 = 2015)",
            xaxis_title="Año",
            yaxis_title="Índice de Crecimiento (Base 100)",
            hovermode="x unified",
        )

        fig2.write_html(os.path.join(data_dir, "graphics", "2_vivienda_vs_salarios.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 3: Brecha Salarial por Ocupación
        # -------------------------------------------------------------------
        print("3/8 Creando barras de Brecha Salarial de Género...")
        df_gap = pl.read_csv(os.path.join(csv_dir, "Brecha_Salarial_Ocupacion.csv"))

        # Nos quedamos con la foto del último año disponible
        last_year = df_gap["anio"].max()
        df_gap_latest = df_gap.filter(pl.col("anio") == last_year)

        # Ordenamos de menor a mayor brecha para que el gráfico quede escalonado visualmente
        df_gap_latest = df_gap_latest.sort("brecha_porcentual")

        fig3 = px.bar(
            df_gap_latest,
            x="brecha_porcentual",
            y="ocupacion",
            orientation="h",  # Barras horizontales para poder leer bien los textos largos
            title=f"Brecha Salarial de Género por Tipo de Ocupación ({last_year})",
            labels={"brecha_porcentual": "Brecha Salarial (%)", "ocupacion": ""},
            color="brecha_porcentual",
            color_continuous_scale="Reds",  # Tonos rojos (más oscuro = más brecha)
            text_auto=".1f",  # Muestra el valor en la barra (con 1 decimal)
        )

        # Añadimos una línea punteada en el 0% por referencia
        fig3.add_vline(x=0, line_dash="dash", line_color="black")

        fig3.update_layout(
            yaxis=dict(tickmode="linear")
        )  # Fuerza a que se lean todas las ocupaciones

        fig3.write_html(os.path.join(data_dir, "graphics", "3_brecha_salarial.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 4: Scatter Plot Animado (Curva Salarial)
        # -------------------------------------------------------------------
        print("4/8 Renderizando animación de la Curva Salarial.")
        df_unemployment_salaries = pl.read_csv(os.path.join(csv_dir, "Relacion_Paro_Salarios.csv"))

        fig4 = px.scatter(
            df_unemployment_salaries,
            x="tasa_paro_media",
            y="salario_medio",
            animation_frame="anio",  # Crea la barra de reproducción por año
            animation_group="comunidad",
            color="comunidad",
            hover_name="comunidad",
            title="Relación entre Tasa de Paro y Salarios por CCAA (Curva Salarial)",
            labels={
                "tasa_paro_media": "Tasa de Paro Media (%)",
                "salario_medio": "Salario Medio Anual (€)",
                "comunidad": "Comunidad Autónoma",
            },
            range_x=[0, 40],
            range_y=[15000, 33000],
            trendline="lowess",
            trendline_scope="overall",  # Hace una línea general para toda España
            trendline_color_override="black",  # Ponemos la línea en negro para que resalte sobre los puntos
        )

        # Ponemos los puntos más grandes y bonitos
        fig4.update_traces(
            marker=dict(size=14, opacity=0.8, line=dict(width=1, color="DarkSlateGrey"))
        )

        # Aumentamos el tiempo de la animación para que no vaya tan rápido
        fig4.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 1000

        fig4.write_html(os.path.join(data_dir, "graphics", "4_paro_vs_salarios.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 4.B: Correlación Paro-Salario por CCAA
        # -------------------------------------------------------------------
        print("5/8 Pintando matriz de correlaciones.")
        df_correlation = pl.read_csv(os.path.join(csv_dir, "Correlacion_Paro_Salarios.csv"))
        
        fig_corr = px.bar(
            df_correlation,
            x="correlacion_pearson",
            y="comunidad",
            orientation="h",
            title="Fuerza de la Curva Salarial: Correlación Paro vs Salario por CCAA",
            labels={
                "correlacion_pearson": "Coeficiente de Correlación de Pearson (r)",
                "comunidad": "",
            },
            # Coloreamos según el valor: azul oscuro para correlaciones muy negativas (fuertes)
            color="correlacion_pearson",
            color_continuous_scale="RdBu",  # Rojo (positivo) a Azul (negativo)
            range_color=[-1, 1],  # La correlación siempre va de -1 a 1
            text_auto=".2f",  # Mostrar el numerito con 2 decimales
        )

        # Añadimos una línea en el 0 para marcar la frontera
        fig_corr.add_vline(x=0, line_width=2, line_dash="solid", line_color="black")

        # Forzamos que se vean todos los nombres de las CCAA
        fig_corr.update_layout(yaxis=dict(tickmode="linear"))

        fig_corr.write_html(os.path.join(data_dir, "graphics", "4b_correlacion_paro_salarios.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 5: Salario Nominal vs Salario Real (Deflactado)
        # -------------------------------------------------------------------
        print("6/8 Visualizando Ilusión Monetaria...")
        df_real_salary = pl.read_csv(os.path.join(csv_dir, "Salario_Nominal_vs_Real.csv"))

        fig5 = go.Figure()

        # 1. Añadimos el Salario Real (Línea sólida verde)
        fig5.add_trace(
            go.Scatter(
                x=df_real_salary["anio"],
                y=df_real_salary["salario_real"],
                mode="lines+markers",
                name="Salario Real (Poder Adquisitivo)",
                line=dict(color="green", width=3),
            )
        )

        # 2. Añadimos el Salario Nominal (Línea punteada roja)
        # El atributo 'fill=tonexty' rellena el hueco hasta la línea verde que pintamos antes
        fig5.add_trace(
            go.Scatter(
                x=df_real_salary["anio"],
                y=df_real_salary["salario_nominal"],
                mode="lines+markers",
                name="Salario Nominal (Euros Brutos en Nómina)",
                line=dict(color="red", width=3, dash="dot"),
                fill="tonexty",
                fillcolor="rgba(255, 0, 0, 0.15)",  # Sombreado rojo translúcido
            )
        )

        # 3. Configuración visual
        fig5.update_layout(
            title="Ilusión Monetaria: Salario Nominal vs Salario Real (Base IPC 2021=100)",
            xaxis_title="Año",
            yaxis_title="Euros (€)",
            hovermode="x unified",  # Al pasar el ratón, compara los dos valores del año de golpe
        )

        fig5.write_html(os.path.join(data_dir, "graphics", "5_salario_nominal_vs_real.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 6: Calidad del Empleo (Área Apilada)
        # -------------------------------------------------------------------
        print("7/8 Generando gráfico de área de Temporalidad.")
        df_job_quality = pl.read_csv(os.path.join(csv_dir, "Calidad_Empleo.csv"))

        fig6 = px.area(
            df_job_quality,
            x="anio",
            y=[
                "Indefinido (%)",
                "Temporal (%)",
            ],  # El orden importa: Indefinido abajo, Temporal arriba
            title="Calidad del Empleo en España: Contratos Indefinidos vs Temporales",
            labels={
                "value": "Porcentaje sobre el total de Asalariados (%)",
                "anio": "Año",
                "variable": "Tipo de Contrato",
            },
            color_discrete_map={"Indefinido (%)": "#2ca02c", "Temporal (%)": "#d62728"},
        )

        # Configuramos el eje Y para que siempre vaya de 0 a 100 exactos
        fig6.update_layout(yaxis=dict(range=[0, 100]), hovermode="x unified")

        fig6.write_html(os.path.join(data_dir, "graphics", "6_calidad_empleo.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 7: Desigualdad Salarial (Distribución de Riqueza)
        # -------------------------------------------------------------------
        print("8/8 Dibujando pirámide de desigualdad salarial...")
        df_inequality = pl.read_csv(os.path.join(csv_dir, "Desigualdad_Salarial.csv"))

        fig7 = px.line(
            df_inequality,
            x="anio",
            y="salario",
            color="indicador",
            markers=True,
            title="Desigualdad Salarial en España: Evolución por Tramos de Ingresos",
            labels={
                "salario": "Salario Anual Bruto (€)",
                "anio": "Año",
                "indicador": "Tramo Salarial",
            },
            # Asignamos colores jerárquicos: Azul (Media, la más alta), Verde (Mediana), Naranja (Cuartil), Rojo (Pobres)
            color_discrete_map={
                "Salario_Anual_Media": "#1f77b4",
                "Salario_Anual_Mediana": "#2ca02c",
                "Salario_Anual_Cuartil inferior": "#ff7f0e",
                "Salario_Anual_Percentil 10": "#d62728",
            },
        )

        # Añadimos interactividad unificada para ver todos los sueldos a la vez al pasar el ratón
        fig7.update_layout(hovermode="x unified")

        fig7.write_html(os.path.join(data_dir, "graphics", "7_desigualdad_salarial.html"))

        # -------------------------------------------------------------------
        # GRÁFICO 8: Evolución del Paro por CCAA (Gráfico Facetado)
        # -------------------------------------------------------------------
        print("Extra: Generando panel facetado (Subplots) de desempleo...")
        fig8 = px.line(
            df_unemployment_salaries,
            x="anio",
            y="tasa_paro_media",
            facet_col="comunidad",
            facet_col_wrap=4,
            title="Evolución de la Tasa de Paro por CCAA (Gráfico Facetado)",
            labels={"tasa_paro_media": "Paro (%)", "anio": ""},
        )

        # Quitamos la etiqueta redundante de "comunidad=" en cada sub-título
        fig8.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))

        fig8.write_html(os.path.join(data_dir, "graphics", "8_paro_facetado.html"))

        print("\nGráficos generado en la carpeta data_output")
    except Exception as e:
        print(f"\nError al generar las visualizaciones: {e}")


if __name__ == "__main__":
    generate_plotly_charts()
