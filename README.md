# Análisis de casos de COVID-19 en Colombia con Apache Spark y Kafka

**Curso:** Big Data (202016911) - UNAD
**Actividad:** Tarea 3 - Procesamiento de Datos con Apache Spark
**Autor:** Daniel Holguín

## Problema

Entre 2020 y 2024 el Instituto Nacional de Salud (INS) registró más de 6,3 millones de casos positivos de COVID-19 en Colombia. Con este volumen de registros, las herramientas tradicionales (hojas de cálculo o pandas en un solo equipo) se quedan cortas para limpiar y analizar los datos completos.

El proyecto usa Apache Spark para responder:

- ¿Cómo evolucionaron los contagios y los fallecimientos mes a mes?
- ¿Qué departamentos concentraron más casos y cuáles tuvieron una letalidad mayor al promedio nacional?
- ¿Cómo cambia la letalidad según el grupo de edad y el sexo?
- ¿Cuántos días pasan en promedio entre el inicio de síntomas y el diagnóstico?

Además simula un sistema de **vigilancia epidemiológica en tiempo real**. Kafka recibe reportes de casos nuevos y Spark Structured Streaming los agrega por departamento en ventanas de 1 minuto. Cuando un departamento acumula casos graves o fallecidos, el sistema genera alertas.

## Conjunto de datos

| | |
|---|---|
| Nombre | Casos positivos de COVID-19 en Colombia |
| Fuente | Instituto Nacional de Salud - [datos.gov.co](https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr) (id `gt2j-8ykr`) |
| Tamaño | ~6.390.000 registros, 23 columnas, más de 1 GB en CSV |
| Periodo | marzo de 2020 a enero de 2024 |
| Columnas principales | fecha de reporte, departamento, municipio, edad, unidad de medida de la edad, sexo, tipo de contagio, estado, recuperado/fallecido, fechas de síntomas, diagnóstico y muerte |

Problemas de calidad encontrados y cómo los resuelve el código:

- **Mayúsculas inconsistentes:** valores como `Leve`, `leve` y `LEVE`, o `CALDAS` y `Caldas`. Se normalizan a mayúsculas.
- **Edades en distintas unidades:** la columna `Unidad de medida de edad` indica 1 = años, 2 = meses, 3 = días. Todas se convierten a años.
- **Valores `N/A` y celdas vacías:** se tratan como nulos.
- **Distritos separados:** Barranquilla, Cartagena y Santa Marta aparecen aparte de su departamento. Se agrupan con Atlántico, Bolívar y Magdalena.
- **Registros inválidos:** se descartan los duplicados por `ID de caso` y los registros sin fecha válida, con edad fuera de rango o con sexo no válido.
- **Diferencias de fechas imposibles:** las diferencias negativas o mayores a 90 días entre síntomas y diagnóstico se anulan.

## Estructura del repositorio

| Archivo | Descripción |
|---|---|
| `descargar_datos.sh` | Descarga el dataset completo o una muestra desde datos.gov.co |
| `batch_covid.py` | Procesamiento batch: carga, limpieza, transformación, EDA (DataFrames, Spark SQL y RDD) y gráficos |
| `kafka_producer_covid.py` | Productor Kafka que genera reportes de casos simulados en el topic `covid_casos` |
| `spark_streaming_covid.py` | Consumidor Spark Structured Streaming con agregaciones por ventana, alertas y tablero PNG |
| `requirements.txt` | Librerías de Python necesarias |

## Requisitos

Se usa la máquina virtual del curso con Hadoop y Spark (Anexo 3):

- Python 3
- Apache Spark 3.5.x (Scala 2.12)
- Apache Kafka 3.9.2 en `/opt/Kafka`
- Librerías de Python:

```bash
pip install -r requirements.txt
```

## 1. Procesamiento batch

### 1.1 Obtener el código y los datos

```bash
git clone https://github.com/<tu-usuario>/covid19-colombia-spark.git
cd covid19-colombia-spark
pip install -r requirements.txt

bash descargar_datos.sh            # dataset completo (~6,4 millones de filas)
# bash descargar_datos.sh 500000   # o una muestra de 500.000 filas para pruebas rápidas
```

Si prefieres trabajar desde HDFS, sube el archivo:

```bash
hdfs dfs -mkdir -p /covid
hdfs dfs -put -f data/casos_covid_colombia.csv /covid/
```

### 1.2 Ejecutar el análisis

```bash
spark-submit --driver-memory 2g batch_covid.py data/casos_covid_colombia.csv
# o desde HDFS:
spark-submit --driver-memory 2g batch_covid.py hdfs://localhost:9000/covid/casos_covid_colombia.csv
```

El programa ejecuta cuatro etapas:

1. **Carga:** lee el CSV, muestra el esquema original y las primeras filas.
2. **Limpieza y transformación:**
   - Renombra columnas.
   - Normaliza textos y convierte fechas.
   - Unifica la edad en años.
   - Crea grupos de edad, el indicador de fallecido y los días entre síntomas y diagnóstico.
   - Elimina duplicados y registros inválidos.
3. **Análisis exploratorio:**
   - Estadísticas descriptivas.
   - Casos y fallecidos por mes.
   - Top de departamentos.
   - Letalidad por grupo de edad, sexo y tipo de contagio.
   - Una consulta con **Spark SQL** de los departamentos con letalidad mayor al promedio nacional.
   - Un cálculo con **RDD** (`filter`, `map`, `reduceByKey`, `take`) de los fallecidos por grupo de edad.
4. **Resultados:**
   - `resultados/casos_limpios.parquet`: datos limpios.
   - `resultados/resumenes/*`: cada resumen en CSV.
   - `resultados/graficos/`: gráficos PNG.
     - `casos_fallecidos_por_mes.png`
     - `top_departamentos.png`
     - `letalidad_por_edad.png`
     - `casos_letalidad_por_sexo.png`

## 2. Procesamiento en tiempo real (Kafka + Spark Streaming)

### 2.1 Iniciar ZooKeeper y Kafka

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```

### 2.2 Crear el topic

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 3 --topic covid_casos
```

### 2.3 Ejecutar el productor (terminal 1)

```bash
python3 kafka_producer_covid.py      # 2 casos por segundo; se puede pasar otra tasa, p. ej. 5
```

Cada evento es un JSON como este:

```json
{"id_evento": 15, "departamento": "VALLE", "edad": 67, "sexo": "F", "tipo_contagio": "COMUNITARIA", "estado": "LEVE", "timestamp": 1761318000}
```

### 2.4 Ejecutar el consumidor (terminal 2)

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_covid.py
```

Cada 10 segundos el consumidor actualiza la ventana de 1 minuto actual y muestra en consola:

- El total nacional de casos, graves y fallecidos de la ventana.
- El ranking de departamentos con casos, fallecidos, graves o fallecidos, edad promedio y porcentaje de mayores de 60 años.
- **Alertas** para los departamentos con 2 o más casos graves o fallecidos en la ventana.

También actualiza dos archivos:

- `resultados_streaming/tablero_tiempo_real.png`: gráfico de la ventana actual.
- `resultados_streaming/ventanas.csv`: histórico de todas las ventanas.

Los jobs y stages se pueden revisar en la interfaz web de Spark: `http://<ip-de-la-vm>:4040`.

Para detener los programas se usa `Ctrl+C` en cada terminal.

> **Nota:** los eventos del streaming son **simulados**, ya que el INS dejó de publicar casos diarios. Para que sean realistas, el productor toma dos datos del análisis batch: la proporción de casos de cada departamento y la letalidad por grupo de edad.

## Referencias

- Instituto Nacional de Salud. (2024). *Casos positivos de COVID-19 en Colombia* [Conjunto de datos]. Datos Abiertos Colombia. https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr
- Apache Software Foundation. (2024). *Structured Streaming Programming Guide*. https://spark.apache.org/docs/3.5.3/structured-streaming-programming-guide.html
- Apache Software Foundation. (2024). *Structured Streaming + Kafka Integration Guide*. https://spark.apache.org/docs/3.5.3/structured-streaming-kafka-integration.html
