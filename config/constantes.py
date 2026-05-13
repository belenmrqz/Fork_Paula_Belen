# Códigos de tablas INE
from pathlib import Path

IPC = 50913
IPV = 25171
ETCL = 6061
EAES_PERCENTILES = 28191
EAES_OCUPACION = 28186
TASA_PARO = 65334
TEMPORALIDAD = 65132

# ── RUTAS ────────────────────────────────────────────────────────────────

CSV_DIR = Path("data_output/csv")
PARQUET_DIR = Path("data_output/parquet")
GRAPHICS_DIR = Path("data_output/graphics")
MODELS_DIR = Path("data_output/models")
GEOJSON_SPAIN = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/spain-communities.geojson"
REPO_URL = "https://github.com/belenmrqz/Fork_Paula_Belen"

# ── ETIQUETAS DE COLUMNAS ────────────────────────────────────────────────
COLUMN_LABELS = {
    "anio": "Año",
    "comunidad": "Comunidad Autónoma",
    "nombre": "Comunidad Autónoma",  # alias en T_empleo tras el join
    "salario_medio": "Salario medio (€)",
    "salario_nominal": "Salario nominal (€/mes)",
    "salario_real": "Salario real (€/mes)",
    "ipc_valor": "IPC anual (índice)",
    "poder_adquisitivo": "Poder adquisitivo",
    "ipv": "Índice Precio Vivienda",
    "indice_salario": "Índice Salario (Base 100)",
    "tasa_paro_media": "Tasa de paro (%)",
    "Hombres": "Salario hombres (€)",
    "Mujeres": "Salario mujeres (€)",
    "brecha_porcentual": "Brecha salarial (%)",
    "ocupacion": "Ocupación",
    "sexo": "Sexo",
    "indicador": "Indicador",
    "salario": "Salario (€)",
    "correlacion_pearson": "Correlación de Pearson",
    "Asalariados_Total": "Asalariados totales (miles)",
    "Asalariados_Temporal": "Asalariados temporales (miles)",
    "Temporal (%)": "Tasa temporalidad (%)",
    "Indefinido (%)": "Tasa indefinidos (%)",
    "precio_vivienda": "IPV regional",
    "valor": "Valor",
    "grupo_edad": "Grupo de edad",
    "tipo_contrato": "Tipo de contrato",
}

# ── PALETAS ──────────────────────────────────────────────────────────────
CLUSTER_COLOR_MAP = {
    "Óptimo": "#2ca02c",
    "Intermedio": "#ffc107",
    "Vulnerable": "#d62728",
}

CLUSTER_ORDER = ["Óptimo", "Intermedio", "Vulnerable"]

GENDER_COLOR_MAP = {
    "Hombres": "#3b82f6",
    "Mujeres": "#ef4444",
}

# ── MAPEO DE NOMBRES DE CCAA (INE → GeoJSON) ─────────────────────────────
CCAA_RENAME_GEOJSON = {
    "Madrid, Comunidad de": "Madrid",
    "Navarra, Comunidad Foral de": "Navarra",
    "Asturias, Principado de": "Asturias",
    "Rioja, La": "La Rioja",
    "Murcia, Región de": "Murcia",
    "Balears, Illes": "Baleares",
    "Comunitat Valenciana": "Valencia",
    "Castilla - La Mancha": "Castilla-La Mancha",
    "Canarias": "Islas Canarias",
    "Andalucía": "Andalucia",
    "Castilla y León": "Castilla-Leon",
    "Aragón": "Aragon",
    "País Vasco": "Pais Vasco",
    "Canarias": "Canarias"
}

CAT_DESCRIPTION_TOOLTIP = {
    "Transporte": "Precio de carburantes, compra de vehículos y transporte público (metro, autobús, vuelos).",
    "Sanidad": "Servicios médicos, seguros de salud, dentistas y productos farmacéuticos.",
    "Ocio y cultura": "Entradas a espectáculos, cine, libros, paquetes turísticos y equipos audiovisuales.",
    "Vestido y calzado": "Prendas de vestir y zapatos (suele fluctuar mucho por la temporada de rebajas).",
    "Enseñanza": "Matrículas universitarias, colegios privados, academias y material escolar.",
    "Muebles, artículos del hogar y artículos para el mantenimiento corriente del hogar": "Electrodomésticos, muebles, herramientas y artículos de limpieza diaria.",
    "Bebidas alcohólicas y tabaco": "Vinos, cervezas, licores y cigarrillos (muy afectado por impuestos especiales).",
    "Otros bienes y servicios": "Seguros de coche/hogar, peluquerías, residencias y cuidados personales.",
}

# ── CATÁLOGO DE DATASETS (explorador de datos) ───────────────────────────
DATASETS = {
    "💼 Evolución Salarial por CCAA": {
        "file": "Evolucion_Salario_Comunidades.csv",
        "desc": "Salario medio anual por Comunidad Autónoma y año. Fuente: EAES (INE Tabla 28191).",
    },
    "📈 Salario Nominal vs Real": {
        "file": "Salario_Nominal_vs_Real.csv",
        "desc": "Comparativa del salario bruto de nómina frente al salario deflactado por el IPC. Muestra la ilusión monetaria.",
    },
    "🏠 Comparativa Vivienda vs Salarios": {
        "file": "Comparativa_Vivienda_Salario.csv",
        "desc": "Evolución del IPV y el salario medio en Base 100 (2015). Permite ver qué crece más rápido.",
    },
    "⚖️ Brecha Salarial por Ocupación": {
        "file": "Brecha_Salarial_Ocupacion.csv",
        "desc": "Diferencia porcentual entre el salario de hombres y mujeres desglosada por tipo de ocupación (CNO-11).",
    },
    "📉 Paro vs Salarios por CCAA": {
        "file": "Relacion_Paro_Salarios.csv",
        "desc": "Cruce entre tasa de paro media y salario medio por región y año. Base de la Curva de Phillips regional.",
    },
    "🔗 Correlación Paro-Salarios (Pearson)": {
        "file": "Correlacion_Paro_Salarios.csv",
        "desc": "Coeficiente de correlación de Pearson entre paro y salario por Comunidad Autónoma.",
    },
    "👷 Calidad del Empleo (Temporalidad)": {
        "file": "Calidad_Empleo.csv",
        "desc": "Proporción de contratos indefinidos vs temporales. Incluye el efecto de la reforma laboral de 2022.",
    },
    "📊 Desigualdad Salarial": {
        "file": "Desigualdad_Salarial.csv",
        "desc": "Evolución de la media, mediana, percentil 10 y cuartil inferior del salario en España.",
    },
    "🤖 Dataset ML (Regresión IPV)": {
        "file": "ML.csv",
        "desc": "Dataset consolidado para los modelos predictivos. Cruza vivienda, salarios, paro e IPC por categorías.",
    },
}