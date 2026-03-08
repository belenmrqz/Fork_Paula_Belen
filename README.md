# 📈 Análisis de la Evolución del Poder Adquisitivo en España

## 🎯 Objetivo del Proyecto

El objetivo principal de este proyecto es analizar la evolución del poder adquisitivo de la clase trabajadora en España mediante un sistema automatizado de **Ingeniería y Análisis de Datos**.

El proyecto implementa un pipeline completo **End-to-End** que abarca:
1.  **Fase 1 (Data Engineering):** Un proceso ETL que recopila datos macroeconómicos de la API del INE y los modela en una base de datos relacional (SQLite).
2.  **Fase 2 (Data Preparation):** Procesamiento multihilo, transformación de datos en memoria utilizando y exportación a CSV/Parquet **Polars**.
3.  **Fase 3 (Data Analytics & Dataviz):** Generación de visualizaciones interactivas de conclusiones con **Plotly**.

El sistema permite desglosar la información por **Comunidades Autónomas**, sectores de actividad y grupos socioeconómicos, demostrando matemáticamente relaciones entre el paro, los salarios y la inflación.

---

## 📊 Fuentes de Datos (INE - API JSON-stat)

El proyecto se alimenta de fuentes oficiales del **Instituto Nacional de Estadística (INE)** mediante peticiones automatizadas a su API. A continuación se enlazan las tablas originales utilizadas:

### 1. Gasto y Coste de Vida
* **[IPC - Índice de Precios de Consumo (Tabla 50913)](https://www.ine.es/jaxiT3/Tabla.htm?t=50913):** Se extrae el índice general, variación anual y categorías clave (Alimentos, Transporte, Energía) para medir la inflación real.
* **[IPV - Índice de Precios de Vivienda (Tabla 25171)](https://www.ine.es/jaxiT3/Tabla.htm?t=25171):** Evolución del precio de compra de vivienda (Índice general y variación anual).

### 2. Ingresos y Salarios
* **[ETCL - Encuesta Trimestral de Coste Laboral (Tabla 6061)](https://www.ine.es/jaxiT3/Tabla.htm?t=6061):** Datos trimestrales sobre el *Coste Salarial* bruto (filtrando costes de seguridad social).
* **EAES - Encuesta Anual de Estructura Salarial:** Datos estructurales para medir la desigualdad.
    * *[Ganancia por trabajador (Tabla 28191)](https://www.ine.es/jaxiT3/Tabla.htm?t=28191):* Media, Mediana, Deciles 10 y Cuartil inferior.
    * *[Ganancia por ocupación (Tabla 28186)](https://www.ine.es/jaxiT3/Tabla.htm?t=28186):* Salarios desglosados por tipo de trabajo.

### 3. Empleo
* **[EPA - Tasa de Paro (Tabla 65334)](https://www.ine.es/jaxiT3/Tabla.htm?t=65334):** Tasa de paro (desglosada por sexo y edad).
* **[Asalariados por tipo de contrato (Tabla 65132)](https://www.ine.es/jaxiT3/Tabla.htm?t=65132):** Datos de asalariados totales vs. temporales (filtrados por jornada total) para calcular la tasa de temporalidad real.

---

## 🗄️ Arquitectura de Datos (Star Schema)

Se ha diseñado una base de datos **SQLite3** siguiendo un **Modelo en Estrella (Star Schema)**. Esta arquitectura separa las *dimensiones* (tablas de búsqueda) de las *tablas de hechos* (datos numéricos), optimizando el análisis posterior.

### Estructura Relacional
La base de datos se organiza en torno a tres tablas centrales de hechos que comparten las mismas dimensiones para facilitar el cruce de datos:

**Tablas de Dimensiones (Lookups):**
* **`tbl_periodo`**: Tabla maestra de tiempo. Normaliza frecuencias mensuales (IPC), trimestrales (EPA) y anuales (EES).
* **`tbl_geografia`**: Comunidades Autónomas y Total Nacional.
* **`tbl_indicador`**: Catálogo unificado de variables (ej: "IPC_General", "Salario_Mediana", "Tasa_Paro").

**Tablas de Hechos (Facts):**
| Tabla | Descripción | Desglose / Segmentación |
| :--- | :--- | :--- |
| **`T_precios`** | Unifica IPC e IPV. | `categoria_gasto` (Alimentos, Vivienda...) |
| **`T_salarios`** | Unifica ETCL y EES. | `sector_cnae`, `ocupacion_cno11`, `sexo` |
| **`T_empleo`** | Unifica Paro y Temporalidad. | `grupo_edad`, `tipo_contrato`, `tipo_jornada` |

```mermaid
erDiagram
    %% --- DIMENSIONES (Tablas Maestras) ---
    tbl_periodo {
        int id_periodo PK
        int anio
        int trimestre
        int mes
        string fecha_iso
    }
    tbl_geografia {
        int id_geografia PK
        string nombre
    }
    tbl_indicador {
        int id_indicador PK
        string nombre
        string unidad
    }

    %% --- HECHOS (Tablas de Datos) ---
    T_precios {
        int id_precio PK
        string categoria_gasto
        float valor
        int id_periodo FK
        int id_geografia FK
        int id_indicador FK
    }
    T_salarios {
        int id_salario PK
        string sexo
        string sector_cnae
        string ocupacion_cno11
        float valor
        int id_periodo FK
        int id_geografia FK
        int id_indicador FK
    }
    T_empleo {
        int id_empleo PK
        string sexo
        string grupo_edad
        string tipo_jornada
        string tipo_contrato
        float valor
        int id_periodo FK
        int id_geografia FK
        int id_indicador FK
    }

    %% --- RELACIONES (Star Schema) ---
    tbl_periodo ||--o{ T_precios : "tiempo"
    tbl_periodo ||--o{ T_salarios : "tiempo"
    tbl_periodo ||--o{ T_empleo : "tiempo"

    tbl_geografia ||--o{ T_precios : "lugar"
    tbl_geografia ||--o{ T_salarios : "lugar"
    tbl_geografia ||--o{ T_empleo : "lugar"

    tbl_indicador ||--o{ T_precios : "metrica"
    tbl_indicador ||--o{ T_salarios : "metrica"
    tbl_indicador ||--o{ T_empleo : "metrica"
```

---

## ⚙️ Arquitectura y Flujo del Pipeline End-to-End

Este proyecto se ha construido siguiendo una arquitectura modular y orientada a objetos (OOP), diseñada para facilitar el mantenimiento, la escalabilidad y la trazabilidad del dato. El proceso completo se divide en tres grandes fases controladas por el orquestador interactivo main.py:

### 1. Estructura de Ficheros
El código se organiza separando claramente la configuración, la lógica de extracción (Fase 1) y el análisis (Fase 2 y 3):

```text
📁 Proyecto
├── 📄 main.py               # Orquestador Central CLI: Bucle ETL, Polars y Plotly.
├── 📁 config
│   └── 📄 constantes.py     # Códigos ID de las tablas API del INE.
├── 📁 src                   # FASE 1: INGESTA Y ALMACENAMIENTO
│   ├── 📄 db.py             # Patrón Singleton para conexión y creación de esquema.
│   ├── 📄 inedata.py        # EXTRACT: Clase para conexión HTTP y descarga JSON.
│   ├── 📄 procesar.py       # TRANSFORM: Limpieza, filtrado y lógica de negocio.
│   └── 📄 almacenar.py      # LOAD: Inserción masiva con control de duplicados.
├── 📁 analysis              # FASE 2 y 3: PREPARACIÓN Y VISUALIZACIÓN
│   ├── 📄 transform.py      # Script de Polars (Deflactación, cruces, agregaciones).
│   └── 📄 visualize.py      # Script de Plotly (Generación de HTMLs interactivos).
├── 📁 data_output           # CAPA ORO: Resultados finales
│   ├── 📁 csv               # Datasets limpios (.csv)
│   ├── 📁 parquet           # Datasets comprimidos (.parquet)
│   ├── 📁 graphics          # Gráficos interactivos de Plotly (.html)
│   └── 📄 index.html        # Dashboard web interactivo para navegar por los gráficos.
├── 📄 proyecto_datos.db     # Base de datos resultante.
├── 📄 requirements.txt      # Dependencias bloqueadas con Hashes de seguridad (uv).
└── 📄 uv.lock               # Archivo de bloqueo de versiones.
```


### 2. Detalle del Proceso de Datos

### Fase 1: Ingeniería de datos e Ingesta

#### 1. Extracción (`src/inedata.py`)
* Conexión HTTP robusta con la API **JSON-stat** del INE.
* Gestión de errores de conexión y tiempos de espera (timeout).
* Descarga de series temporales completas en formato crudo (raw data).

#### 2. Transformación (`src/procesar.py`)
Es la etapa más compleja, donde se aplica la lógica de negocio para asegurar la calidad del dato:
* **Parsing de Metadatos:** Se descomponen las cadenas de texto del INE (ej: *"Total Nacional. Industria. Coste..."*) para extraer dimensiones limpias (Geografía, Sector, Sexo).
* **Filtrado de Salarios:** Se discrimina entre *"Coste Laboral"* y *"Coste Salarial"*, conservando únicamente este último (salario bruto) para reflejar la remuneración real del trabajador.
* **Lógica de Empleo:** Se filtran los datos de jornada parcial para calcular la **Temporalidad** basándose exclusivamente en contratos de jornada completa (comparando *Total Asalariados* vs *Temporales*).
* **Normalización del IPC:** Se agrupan y renombran las categorías de gasto (Alimentos, Vivienda, Transporte) para facilitar consultas SQL posteriores.

#### 3. Carga (`src/almacenar.py`)
* **Enrutamiento Inteligente:** El sistema detecta automáticamente a qué tabla de hechos (`T_precios`, `T_salarios`, `T_empleo`) deben ir los datos según su código de origen.
* **Gestión de Integridad:** Uso de sentencias `INSERT OR IGNORE` combinadas con claves únicas compuestas (`UNIQUE`) en la base de datos. Esto permite re-ejecutar el script tantas veces como sea necesario sin generar registros duplicados.

---

### Fase 2: Data Preparation (Polars)

Para optimizar el rendimiento y demostrar el dominio técnico de la herramienta, se realizaron únicamente **3 consultas maestras** a la base de datos SQLite (extrayendo los bloques en bruto de Salarios, Precios y Empleo). A partir de ahí, se delegó toda la carga analítica a **Polars** (`analysis/transform.py`), realizando en memoria todos los **filtrados, pivotados (reshaping) y agregaciones** necesarios.

Durante esta fase de transformación se resolvieron los siguientes retos:

* **Normalización a Base 100:** Para poder comparar magnitudes económicas con unidades distintas (ej. euros de salario vs. índice de precios de vivienda), se transformaron las series temporales a **Base 100** tomando como referencia el año correspondiente, permitiendo analizar de forma relativa la "carrera" de su crecimiento.
* **Ilusión Monetaria y Correlaciones:** Se deflactaron los salarios cruzándolos con el IPC General y se calcularon matrices de correlación de Pearson entre la tasa de paro y los salarios por CCAA (Curva de Phillips).
* **Gestión de Secretos Estadísticos:** Se detectaron y limpiaron salarios con valores negativos ocultos por el INE en la tabla de ocupaciones, evitando distorsiones graves en el cálculo de la brecha de género.
* **Exportació:n** Los resultados finales (Capa Oro) se exportaron a `.csv` y, como mejora adicional, a **`.parquet`**. El guardado columnar de Polars en formato Parquet reduce drásticamente el peso del archivo y acelera exponencialmente futuras lecturas.

### Fase 3: Análisis Visual y Conclusiones (Plotly)

A partir de la Capa Oro, el script `analysis/visualize.py` genera visualizaciones interactivas (disponibles en `/data_output/graphics/` o desde `/data_output/index.html`). 

A continuación, se exponen las capturas de los gráficos principales y las conclusiones extraídas de los datos:

#### 1. Evolución Salarial por CCAA (2008-2023)
![Evolución Salarios](assets/evolucion_salarios.png)

* **Crecimiento General**: Los salarios nominales han subido en todas las regiones desde 2008, acelerándose tras la pandemia (2020).
* **Brecha Territorial:** Se mantiene una jerarquía clara: **País Vasco y Madrid** lideran (32k€+), mientras que **Extremadura y Canarias** cierran la lista (<24k€). La distancia entre las regiones más ricas y pobres se ha ensanchado.
* **Hito Crítico**: Se observa un estancamiento salarial prolongado entre **2010 y 2015** debido a la crisis financiera.

#### 2. La Carrera: Salarios vs Precio de la Vivienda
![Salarios vs Vivienda](assets/vivienda_vs_salarios.png)
* **Crecimiento Desigual**: Desde 2015, el precio de la vivienda ha subido un **50%**, mientras que los salarios solo han crecido un **20%**.
* **Brecha de Accesibilidad**: La vivienda crece a más del **doble de velocidad** que los sueldos, lo que supone una barrera cada vez mayor para el ahorro y la compra de inmuebles.
* **Tendencia**: Tras la caída de precios entre 2008 y 2014, la recuperación del mercado inmobiliario ha desbordado por completo la capacidad adquisitiva de los trabajadores.

#### 3. Brecha Salarial de Género por Ocupación (2023)
![Brecha Salarial](assets/brecha_salarial.png)
* **Desigualdad Generalizada**: Todas las ocupaciones presentan una brecha salarial positiva a favor de los hombres, oscilando entre el **7,5% y el 25,4%**.
* **Sectores Críticos**: La mayor brecha se concentra en trabajos manuales e industriales (**25%**), como operarios y trabajadores cualificados de la manufactura.
* **Menor Disparidad**: Los sectores de enseñanza, salud y perfiles técnicos cualificados presentan la brecha más baja (**7,5% - 14%**), aunque sigue siendo significativa.

#### 4. Curva Salarial Regional (Paro vs Salarios)
![Correlación Paro Salarios(2016)](assets/paro_vs_salarios.png)
* **Tendencia Inversa**: El gráfico muestra una clara pendiente negativa; visualmente se confirma que las regiones con menores tasas de desempleo (como Madrid o País Vasco) logran sostener salarios mucho más altos.
* **Dualidad Regional**: Existe una brecha evidente entre el bloque de bajo paro/salario alto frente a las regiones con desempleo estructural, donde los salarios medios se ven presionados a la baja, estancándose cerca de los 20k-22k€.

Para verificar matemáticamente esta correlación negativa se calculó la Correlación de Pearson:

![Correlación de Pearson](assets/correlacion_paro.png)
* **Confirmación Matemática**: Todos los coeficientes son negativos (entre **-0,41 y -0,64**), lo que demuestra que la relación inversa no es casualidad, sino una constante en toda España.
* **Sensibilidad Regional**: En regiones como **Castilla y León (-0,64)** y **Canarias (-0,61)**, el mercado laboral es más sensible: el paro tiene un impacto mucho más directo y fuerte en el frenado de los salarios que en otras comunidades.

#### 5. Ilusión monetaria: Salario nominal vs Salario Real (Base IPC 2021 = 100)
![Salario nominal vs real)](assets/salario_nominal_vs_real.png)
* **Divergencia Evidente:** Mientras que el **salario nominal** (euros brutos en nómina) muestra un crecimiento fuertemente acelerado en los últimos años, el **salario real** (poder adquisitivo) presenta una tendencia general a la baja desde su pico máximo cerca de 2009.

* **El Punto de Inflexión (2021):** El año 2021 marca el cruce exacto de ambas líneas (al ser el año base del IPC). A partir de este punto, la inflación genera una enorme brecha: los sueldos sobre el papel suben drásticamente, pero la capacidad real de compra cae en picado.

* **Pérdida de Poder Adquisitivo:** A pesar de que en 2025 un trabajador cobra nominalmente unos 550€ más que al inicio de la serie (pasando de ~1.810€ a >2.350€), su poder adquisitivo real es inferior al que tenía hace más de 15 años, cayendo por debajo de los 2.000€.

#### 6. Calidad del Empleo en España: Contratos Indefinidos vs Temporales 
![Calidad del empleo)](assets/calidad_empleo.png)
* **Pico de Temporalidad Histórica:** En los primeros años de la serie (hasta 2006 aproximadamente), el empleo temporal presentaba su mayor volumen, con los contratos indefinidos representando apenas el **66-67%** del total de asalariados.

* **Estancamiento en la Década (2010-2020):** Durante estos años, la proporción de contratos indefinidos se mantuvo relativamente estable pero estancada, oscilando entre el **73% y el 76%.** Esto reflejaba una tasa de temporalidad estructural que no lograba bajar del 24-25%.

* **Cambio de Tendencia Drástico:** A partir de 2021-2022 se observa una escalada abrupta en la proporción de contratos indefinidos. La gráfica muestra cómo esta tasa rompe la barrera del 80% y continúa subiendo hasta acercarse al **85%** en 2025, lo que comprime la cuota de contratos temporales a mínimos de la serie (en torno al 15%).

#### 7. Desigualdad Salarial en España: Evolución por Tramos de Ingresos
![Desigualdad Salarial)](assets/desigualdad_salarial.png)
* **Brecha entre Media y Mediana:** El salario medio (línea azul) se sitúa sistemáticamente por encima del salario mediano (línea verde) durante toda la serie histórica. Esto indica una distribución asimétrica donde los sueldos más altos tiran del promedio general hacia arriba: en 2023, la media se acerca a los 28.000€, mientras que la mediana (el salario más frecuente) se queda en torno a los 23.000€.

* **Impacto en las Rentas Bajas:** El percentil 10 (la línea roja, que representa al 10% que menos cobra) sufrió una ligera caída durante los años posteriores a 2008, tocando fondo alrededor de 2014 por debajo de los 8.000€ anuales. Sin embargo, en los últimos años muestra una fuerte recuperación, superando los 11.000€ en 2023.

* **Crecimiento Generalizado Reciente:** Tras un periodo de estancamiento general entre 2008 y 2014, a partir de 2016 todos los tramos salariales inician una clara tendencia alcista. Esta subida nominal se acelera especialmente en el último tramo de la gráfica (2020-2023) para todos los niveles de ingresos.

#### 8. Evolución de la Tasa de Paro por CCAA (Gráfico Facetado)
![Paro facetado)](assets/paro_facetado.png)
* **Patrón Común de Crisis y Recuperación:** Todas las comunidades autónomas muestran una curva evolutiva muy similar, marcada por un fuerte aumento del desempleo que alcanza su punto máximo alrededor de los años 2013 y 2014, seguido de un descenso progresivo y generalizado en la última década.

* **Las Tasas Históricas Más Altas:** Regiones como Andalucía, Canarias y Extremadura destacan por haber sufrido los impactos más severos durante el pico de la crisis, superando claramente la barrera del 30% de paro.

* **Comunidades con Menor Impacto:** En el extremo opuesto, el País Vasco, Navarra, Aragón y La Rioja presentan las curvas más planas y contenidas. En estas zonas, los picos máximos apenas rozaron el 20% y actualmente cierran la serie temporal situándose en torno al 10% o incluso por debajo.

___
## Métricas y Visualización Exploratoria con Tableau

Una vez finalizada la fase de procesamiento y estructuración de datos con Polar, entramos en la etapa de **Explotación de datos**. 
Utilizaremos la herramienta de Business Intelligente (BI) **Tableau** para generar informes gráficos sobre los datos procesados anteriormente. 

### Paso 1: Genenrar una conexión híbrida

#### Carga de archivos:
Importaremos al programa de Tableau los CSV que procesamos anteriormente con Polars

#### Conexión a la BBDD: 

### 📊 Visualizaciones individuales
Con el objetivo de experimentar la creación de gráficas y aplicar parte de los muchos filtros y funciones que tiene la herramienta Tableau hemos replicado los 9 gráficos que creamos con Plotly. 

Sin embargo, hemos experimentado con 3 gráficos nuevos:

1. **Tasa de paro de Hombres vs Mujeres por edad**

![Tableau_01)](assets/Tableau_TasaParo_HombresMujeres.png)

Esta gráfica de barras agrupadas desglosa el problema del desempleo en España con una lupa doble: nos muestra el porcentaje de paro separado no solo por hombres y mujeres, sino también por tramos de edad.

Es una visualización clave para desmontar el "paro general" y ver la realidad de los más vulnerables. Por ejemplo, a simple vista destaca cómo el desempleo juvenil (de 16 a 19 años) se dispara radicalmente frente al resto, castigando además con mucha más dureza a las mujeres jóvenes.

**🎛️ ¿Cómo interactuar con ella?**
La gráfica cuenta con dos selectores a la derecha para que puedas explorar los datos a tu medida:

- 🎚️ El viaje en el tiempo (Año): Puedes mover el deslizador del año o escribir uno en concreto (ej. 2025) para comprobar si la brecha de género está mejorando o empeorando con el paso del tiempo.

- 📍 El filtro local (Comunidad Autónoma): Desmarca la casilla "(Todo)" y elige solo tu Comunidad Autónoma para descubrir si en tu región hay más paro juvenil o femenino que en la media nacional.



2. **Impacto de la Inflación por Categoría de Gasto**

![Tableau_02)](assets/Tableau_Inflaccion.png)

Esta gráfica es el "termómetro" real de nuestros bolsillos. Nos muestra cómo han evolucionado los precios de las cosas que pagamos en el día a día (comida, luz, sanidad, ocio, etc.) desde el año 2002 hasta la actualidad.

Para que sea súper fácil de leer, hemos aplicado un código de colores térmico: el color verde indica los años en los que los precios eran bajos y manejables, mientras que el rojo intenso funciona como una señal de alarma que marca cuándo los precios se han disparado. A simple vista, es imposible no notar la "ola roja" que asfixia categorías de primera necesidad (como Alimentos o Vivienda) en los últimos años, explicando visualmente por qué a las familias les cuesta cada vez más llegar a fin de mes, aunque sus sueldos hayan subido.

**🎛️ ¿Cómo interactuar con ella?**

- 🎚️ Monta tu propia cesta de la compra (Selector de categorías): En el menú de la derecha puedes marcar o desmarcar casillas para analizar solo los gastos que te interesen. ¿Quieres comparar si ha subido más la comida o la factura del teléfono (Comunicaciones)? Selecciona solo esas dos y descúbrelo.

- 🔍 El dato exacto a un clic (Zoom-in): Si ves un bloque muy rojo (por ejemplo, los Alimentos en 2024) y quieres saber el dato preciso, solo tienes que pasar el ratón por encima de esa barra para que se despliegue una tarjeta con los valores exactos.



### 🗺️ Dashboard (Cuadro de Mando)

El objetivo de este dashboard interactivo es ofrecer una visión integral y territorial de la realidad económica de los ciudadanos. En lugar de analizar métricas aisladas, este panel conecta el mercado laboral (empleo), los ingresos (salarios) y el coste de la vida (inflación) para entender el verdadero poder adquisitivo y la vulnerabilidad económica de la población.

![Tableau_Dashboard_01)](assets/Tableau_Dashboard_01.png)


![Tableau_Dashboard_02)](assets/Tableau_Dashboard_02.png)

#### Gráficos seleccionados:
1. **El Mapa de España (El filtro principal del panel):**
España es muy diferente según donde vivas. Si haces clic en tu Comunidad Autónoma en el mapa, las otras tres gráficas se actualizan al instante para mostrarte en exclusiva la realidad de tu zona.

2. **El Paro por Edad y Sexo:**
Nos enseña visualmente quiénes son los más vulnerables. Permite ver de un vistazo si el problema lo tienen más los jóvenes para encontrar su primer empleo, o si existe una brecha clara entre hombres y mujeres.

3. **Evolución del Salario Medio:**
Representa "el dinero que entra en casa". Nos sirve para ver si los salarios están creciendo o si, por el contrario, llevan años estancados.

4. **La Inflación por Categorías:**
Representa "el dinero que sale de casa". Es la pareja perfecta para la gráfica del salario: de nada sirve alegrarnos porque el sueldo suba un poco, si al lado vemos que el precio de los alimentos o la luz ha subido el doble. Esto explica por qué a la gente le cuesta más llegar a fin de mes.
___

## 🚀 Instalación y Uso

Sigue estos pasos para desplegar el proyecto en tu entorno local:

### 1. Clonar el repositorio
Descarga el código fuente desde GitHub:
```bash
git clone https://github.com/belenmrqz/Fork_Paula_Belen
cd Fork_Paula_Belen
```

### 2. Configurar el entorno virtual con `uv`
Se recomiendo usar `uv` por su velocidad en la gestión de dependencias:
* **En macOS / Linux**:
    ```bash
    uv venv
    source .venv/bin/activate
    ```
* **En Windows**:
    ```bash
    uv venv
    .venv\Scripts\activate
    ```

### 3. Instalar dependencias bloqueadas
Instala las librerías utilizando el archivo que contiene los hashes de seguridad:
```bash
uv pip install -r requirements.txt
```

### 4. Ejecutar el Orquestados
Lanza el script principal. Aparecerá un panel de control interactivo en la terminal. Pulsa la opción 4 para ejecutar el pipeline completo de principio a fin:
```bash
python main.py
```
---

## 🤝 Colaboradores

* Belén Márquez López -> https://github.com/belenmrqz
* Paula Sánchez Vélez -> https://github.com/paulaschez

* (Fase 1) Alejandro Bernabé Guerrero -> https://github.com/Alebernabe5
* (Fase 1) Ivana Sánchez Pérez -> https://github.com/Ivanasp43



