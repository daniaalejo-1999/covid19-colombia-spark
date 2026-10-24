"""
Procesamiento batch de los casos positivos de COVID-19 en Colombia con PySpark.

Fuente: Instituto Nacional de Salud (INS) - Portal de Datos Abiertos de Colombia
        https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr

Problema: identificar patrones de contagio y letalidad del COVID-19 en Colombia
          segun departamento, grupo de edad, sexo y periodo de tiempo.

Uso:
    spark-submit batch_covid.py data/casos_covid_colombia.csv
    spark-submit batch_covid.py hdfs://localhost:9000/covid/casos_covid_colombia.csv
"""

import sys
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

RUTA_SALIDA = "resultados"

# Los encabezados originales tienen tildes y espacios; se renombran a snake_case.
COLUMNAS = {
    "fecha reporte web": "fecha_reporte",
    "ID de caso": "id_caso",
    "Fecha de notificación": "fecha_notificacion",
    "Código DIVIPOLA departamento": "cod_departamento",
    "Nombre departamento": "departamento",
    "Código DIVIPOLA municipio": "cod_municipio",
    "Nombre municipio": "municipio",
    "Edad": "edad",
    "Unidad de medida de edad": "unidad_edad",
    "Sexo": "sexo",
    "Tipo de contagio": "tipo_contagio",
    "Ubicación del caso": "ubicacion",
    "Estado": "estado",
    "Código ISO del país": "cod_pais",
    "Nombre del país": "pais",
    "Recuperado": "recuperado",
    "Fecha de inicio de síntomas": "fecha_inicio_sintomas",
    "Fecha de muerte": "fecha_muerte",
    "Fecha de diagnóstico": "fecha_diagnostico",
    "Fecha de recuperación": "fecha_recuperacion",
    "Tipo de recuperación": "tipo_recuperacion",
    "Pertenencia étnica": "pertenencia_etnica",
    "Nombre del grupo étnico": "grupo_etnico",
}

# Nombres de columna cuando el CSV se descarga desde la API (muestra con $limit).
# El orden importa: "departamento" (codigo) se renombra antes que "departamento_nom".
COLUMNAS_API = {
    "fecha_reporte_web": "fecha_reporte",
    "id_de_caso": "id_caso",
    "fecha_de_notificaci_n": "fecha_notificacion",
    "departamento": "cod_departamento",
    "departamento_nom": "departamento",
    "ciudad_municipio": "cod_municipio",
    "ciudad_municipio_nom": "municipio",
    "unidad_medida": "unidad_edad",
    "fuente_tipo_contagio": "tipo_contagio",
    "fecha_muerte": "fecha_muerte",
    "fecha_recuperado": "fecha_recuperacion",
    "per_etn_": "pertenencia_etnica",
    "nom_grupo_": "grupo_etnico",
}

# El INS reporta Barranquilla, Cartagena y Santa Marta como distritos aparte.
# Para el analisis por departamento se suman a su departamento.
DISTRITOS = {
    "BARRANQUILLA": "ATLANTICO",
    "CARTAGENA": "BOLIVAR",
    "STA MARTA D.E.": "MAGDALENA",
}

FORMATO_FECHA = "yyyy-MM-dd HH:mm:ss"


def crear_sesion():
    spark = SparkSession.builder.appName("BatchCovidColombia").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def cargar_datos(spark, ruta):
    """Carga el CSV original con todas las columnas como texto (se tipan en la limpieza)."""
    df = spark.read.option("header", "true").option("encoding", "UTF-8").csv(ruta)
    mapa = COLUMNAS if "ID de caso" in df.columns else COLUMNAS_API
    for original, nuevo in mapa.items():
        if original in df.columns:
            df = df.withColumnRenamed(original, nuevo)
    return df


def limpiar_datos(df):
    """Normaliza textos, convierte tipos, unifica la edad en anios y elimina registros invalidos."""
    texto = ["departamento", "municipio", "sexo", "tipo_contagio", "ubicacion",
             "estado", "recuperado", "tipo_recuperacion"]
    for c in texto:
        df = df.withColumn(c, F.upper(F.trim(F.col(c))))

    # "N/A" y cadenas vacias se tratan como nulos
    for c in ["estado", "ubicacion", "recuperado", "tipo_recuperacion"]:
        df = df.withColumn(c, F.when(F.col(c).isin("N/A", ""), None).otherwise(F.col(c)))

    for c in ["fecha_reporte", "fecha_notificacion", "fecha_inicio_sintomas",
              "fecha_muerte", "fecha_diagnostico", "fecha_recuperacion"]:
        df = df.withColumn(c, F.to_date(F.to_timestamp(F.col(c), FORMATO_FECHA)))

    # Unidad de medida de la edad: 1 = anios, 2 = meses, 3 = dias. Otros valores se descartan.
    edad = F.col("edad").cast("double")
    df = df.withColumn(
        "edad_anios",
        F.when(F.col("unidad_edad") == "1", edad)
         .when(F.col("unidad_edad") == "2", edad / 12)
         .when(F.col("unidad_edad") == "3", edad / 365)
         .cast(IntegerType()),
    )

    df = df.withColumn("id_caso", F.col("id_caso").cast("long"))
    df = df.withColumn("fallecido", F.when(F.col("recuperado") == "FALLECIDO", 1).otherwise(0))

    mapa_distritos = F.create_map(*[F.lit(x) for par in DISTRITOS.items() for x in par])
    df = df.withColumn(
        "departamento_agrupado",
        F.coalesce(mapa_distritos[F.col("departamento")], F.col("departamento")),
    )

    df = df.withColumn(
        "grupo_edad",
        F.when(F.col("edad_anios") < 10, "00-09")
         .when(F.col("edad_anios") < 20, "10-19")
         .when(F.col("edad_anios") < 30, "20-29")
         .when(F.col("edad_anios") < 40, "30-39")
         .when(F.col("edad_anios") < 50, "40-49")
         .when(F.col("edad_anios") < 60, "50-59")
         .when(F.col("edad_anios") < 70, "60-69")
         .when(F.col("edad_anios") < 80, "70-79")
         .otherwise("80+"),
    )

    # Diferencias negativas o mayores a 90 dias se consideran errores de digitacion
    dias = F.datediff(F.col("fecha_diagnostico"), F.col("fecha_inicio_sintomas"))
    df = df.withColumn("dias_sintomas_diagnostico", F.when(dias.between(0, 90), dias))

    df = (df.dropDuplicates(["id_caso"])
            .filter(F.col("id_caso").isNotNull())
            .filter(F.col("fecha_reporte").isNotNull())
            .filter(F.col("edad_anios").between(0, 115))
            .filter(F.col("sexo").isin("F", "M")))

    return df.select(
        "id_caso", "fecha_reporte", "fecha_inicio_sintomas", "fecha_diagnostico", "fecha_muerte",
        "departamento", "departamento_agrupado", "municipio", "edad_anios", "grupo_edad", "sexo",
        "tipo_contagio", "ubicacion", "estado", "recuperado", "fallecido", "dias_sintomas_diagnostico",
    )


def letalidad(df, columna):
    """Casos, fallecidos y tasa de letalidad (%) agrupando por una columna."""
    return (df.groupBy(columna)
              .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
              .withColumn("letalidad_pct", F.round(F.col("fallecidos") / F.col("casos") * 100, 2)))


def analisis_exploratorio(df, spark):
    resultados = {}

    print("\n=== Estadisticas descriptivas de la edad ===")
    df.select("edad_anios", "dias_sintomas_diagnostico").describe().show()

    resultados["por_mes"] = (df.withColumn("mes", F.date_format("fecha_reporte", "yyyy-MM"))
                               .groupBy("mes")
                               .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
                               .orderBy("mes"))
    print("\n=== Casos y fallecidos por mes (primeros 24 meses) ===")
    resultados["por_mes"].show(24)

    resultados["por_departamento"] = letalidad(df, "departamento_agrupado").orderBy(F.desc("casos"))
    print("\n=== Top 15 departamentos por numero de casos ===")
    resultados["por_departamento"].show(15, truncate=False)

    resultados["por_grupo_edad"] = letalidad(df, "grupo_edad").orderBy("grupo_edad")
    print("\n=== Letalidad por grupo de edad ===")
    resultados["por_grupo_edad"].show()

    resultados["por_sexo"] = letalidad(df, "sexo")
    print("\n=== Letalidad por sexo ===")
    resultados["por_sexo"].show()

    resultados["por_tipo_contagio"] = letalidad(df, "tipo_contagio").orderBy(F.desc("casos"))
    print("\n=== Casos por tipo de contagio ===")
    resultados["por_tipo_contagio"].show()

    # Spark SQL: departamentos con letalidad por encima del promedio nacional
    df.createOrReplaceTempView("casos")
    resultados["letalidad_alta"] = spark.sql("""
        SELECT departamento_agrupado,
               COUNT(*) AS casos,
               SUM(fallecido) AS fallecidos,
               ROUND(SUM(fallecido) / COUNT(*) * 100, 2) AS letalidad_pct
        FROM casos
        GROUP BY departamento_agrupado
        HAVING SUM(fallecido) / COUNT(*) > (SELECT SUM(fallecido) / COUNT(*) FROM casos)
        ORDER BY letalidad_pct DESC
    """)
    print("\n=== Departamentos con letalidad mayor al promedio nacional (Spark SQL) ===")
    resultados["letalidad_alta"].show(truncate=False)

    # RDD: el mismo conteo con transformaciones (map, filter, reduceByKey) y acciones (take, count)
    rdd = df.select("grupo_edad", "fallecido").rdd
    fallecidos_rdd = (rdd.filter(lambda fila: fila.fallecido == 1)
                         .map(lambda fila: (fila.grupo_edad, 1))
                         .reduceByKey(lambda a, b: a + b)
                         .sortBy(lambda par: par[1], ascending=False))
    print("\n=== Fallecidos por grupo de edad calculado con RDD (map + reduceByKey) ===")
    for grupo, total in fallecidos_rdd.take(10):
        print(f"  {grupo}: {total:,}")
    print(f"  Particiones del RDD: {rdd.getNumPartitions()}")

    return resultados


def guardar_resultados(df, resultados):
    df.write.mode("overwrite").parquet(os.path.join(RUTA_SALIDA, "casos_limpios.parquet"))
    for nombre, resumen in resultados.items():
        (resumen.coalesce(1).write.mode("overwrite").option("header", "true")
                .csv(os.path.join(RUTA_SALIDA, "resumenes", nombre)))
    print(f"\nResultados guardados en la carpeta '{RUTA_SALIDA}/'")


def graficar(resultados):
    """Los resumenes son pequenos, asi que se pasan a pandas para graficarlos con matplotlib."""
    try:
        import pandas  # noqa: F401  (lo necesita toPandas)
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib no esta instalado: se omiten los graficos (pip install matplotlib pandas)")
        return

    carpeta = os.path.join(RUTA_SALIDA, "graficos")
    os.makedirs(carpeta, exist_ok=True)

    mes = resultados["por_mes"].toPandas()
    fig, ax1 = plt.subplots(figsize=(13, 5))
    ax1.plot(mes["mes"], mes["casos"], color="tab:blue", label="Casos")
    ax1.set_ylabel("Casos", color="tab:blue")
    ax2 = ax1.twinx()
    ax2.plot(mes["mes"], mes["fallecidos"], color="tab:red", label="Fallecidos")
    ax2.set_ylabel("Fallecidos", color="tab:red")
    ax1.set_xticks(range(0, len(mes), 3))
    ax1.set_xticklabels(mes["mes"][::3], rotation=60)
    ax1.set_title("Casos y fallecidos por COVID-19 en Colombia por mes de reporte")
    fig.tight_layout()
    fig.savefig(os.path.join(carpeta, "casos_fallecidos_por_mes.png"), dpi=120)
    plt.close(fig)

    dep = resultados["por_departamento"].limit(15).toPandas().iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(dep["departamento_agrupado"], dep["casos"], color="tab:blue")
    ax.set_title("Top 15 departamentos por casos de COVID-19")
    ax.set_xlabel("Casos")
    fig.tight_layout()
    fig.savefig(os.path.join(carpeta, "top_departamentos.png"), dpi=120)
    plt.close(fig)

    edad = resultados["por_grupo_edad"].toPandas()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(edad["grupo_edad"], edad["letalidad_pct"], color="tab:red")
    ax.set_title("Tasa de letalidad (%) por grupo de edad")
    ax.set_xlabel("Grupo de edad")
    ax.set_ylabel("Letalidad (%)")
    fig.tight_layout()
    fig.savefig(os.path.join(carpeta, "letalidad_por_edad.png"), dpi=120)
    plt.close(fig)

    sexo = resultados["por_sexo"].toPandas()
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].pie(sexo["casos"], labels=sexo["sexo"], autopct="%1.1f%%")
    axes[0].set_title("Casos por sexo")
    axes[1].bar(sexo["sexo"], sexo["letalidad_pct"], color="tab:red")
    axes[1].set_title("Letalidad (%) por sexo")
    fig.tight_layout()
    fig.savefig(os.path.join(carpeta, "casos_letalidad_por_sexo.png"), dpi=120)
    plt.close(fig)

    print(f"Graficos guardados en '{carpeta}/'")


def main():
    if len(sys.argv) < 2:
        print("Uso: spark-submit batch_covid.py <ruta_csv>")
        sys.exit(1)

    spark = crear_sesion()

    print("=== 1. Carga de datos ===")
    df_original = cargar_datos(spark, sys.argv[1])
    total_original = df_original.count()
    print(f"Registros cargados: {total_original:,}")
    df_original.printSchema()
    df_original.show(5, truncate=False)

    print("=== 2. Limpieza y transformacion ===")
    df = limpiar_datos(df_original).cache()
    total_limpio = df.count()
    print(f"Registros despues de la limpieza: {total_limpio:,} "
          f"(descartados: {total_original - total_limpio:,})")
    df.show(5, truncate=False)

    print("=== 3. Analisis exploratorio ===")
    resultados = analisis_exploratorio(df, spark)

    print("=== 4. Resultados y visualizacion ===")
    guardar_resultados(df, resultados)
    graficar(resultados)

    spark.stop()


if __name__ == "__main__":
    main()
