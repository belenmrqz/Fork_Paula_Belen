# 📈 Análisis de la Evolución del Poder Adquisitivo en España

## 🎯 Objetivo del Proyecto

El objetivo principal de este proyecto es analizar la evolución del poder adquisitivo de la clase trabajadora en España mediante la recopilación y el análisis de series de datos macroeconómicos clave. Se busca establecer la relación entre los niveles de empleo, la remuneración y el coste de vida.

## 📊 Fuentes de Datos (INE - Instituto Nacional de Estadística)

Para este análisis, se han consultado las APIs públicas del INE, garantizando la veracidad y la obtención automatizada de los datos:

* **IPC (Índice de Precios al Consumo):** Utilizado para medir la variación anual del coste de vida (inflación).
* **PIB (Producto Interior Bruto):** Utilizado para medir la variación anual de la actividad económica.
* **Tasa de Paro (EPA):** Empleado para analizar la evolución del nivel de empleo.
* **Salario Medio (Encuesta de Estructura Salarial):** Recopilado anualmente por sexo (Decil 5) para estimar la evolución de la remuneración.

## 🗄️ Estructura de Almacenamiento

Se ha optado por una **Base de Datos Relacional SQLite3** (`proyecto_datos.db`) para el almacenamiento de los datos debido a su sencillez, portabilidad y la naturaleza estructurada y relacional de las series temporales.

### 🏛️ Diseño de la Base de Datos

La base de datos se compone de las siguientes tablas, con `tbl_periodo` como la tabla maestra de fechas para asegurar la consistencia temporal:

| Tabla | Contenido | Claves |
| :--- | :--- | :--- |
| **tbl_periodo** | Contiene todos los períodos (mensuales/anuales) de las series. | `id_periodo` (PK), `fecha_iso` (UNIQUE) |
| **tbl_ipc** | Almacena la variación anual general del IPC (mensual). | `id_periodo` (PK, FK) |
| **tbl_pib** | Almacena la variación anual del PIB (anual). | `id_periodo` (PK, FK) |
| **tbl_paro** | Tasa de paro nacional, segmentada por sexo (anual). | `id_periodo` (PK, FK), `sexo` (PK) |
| **tbl_salario** | Salario bruto medio por decil, segmentado por sexo (anual). | `id_periodo` (PK, FK), `decil` (PK) |

## ⚙️ Automatización del Proceso

El proceso de ETL (Extracción, Transformación y Carga) está automatizado mediante dos scripts de Python:

1.  **`db_setup.py`**: Se encarga de crear la estructura de la base de datos (`proyecto_datos.db`) y sus tablas.
2.  **`cargar_datos.py`**: 
    * **Extracción:** Realiza peticiones HTTP (requests) a los endpoints de la API del INE.
    * **Transformación:** Normaliza los datos, calcula métricas (como la variación anual del IPC) y determina los IDs de período. Utiliza **pandas** para el manejo de datos tabulares.
    * **Carga:** Almacena la información en las tablas de SQLite, gestionando la inserción de nuevos períodos y la actualización de datos existentes (`INSERT OR REPLACE`).

## 🤝 Colaboradores

* Alejandro Bernabé Guerrero
* Belén Márquez López
* Ivana Sánchez Pérez
* Paula Sánchez Vélez

## ▶️ Próximos Pasos

El siguiente paso será realizar el análisis de datos (Paso 8) mediante consultas SQL y/o código Python (e.g., usando Pandas o librerías de visualizac



