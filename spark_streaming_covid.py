"""
Consumidor Spark Structured Streaming: analiza en tiempo real los casos de COVID-19
que llegan al topic de Kafka 'covid_casos'.

Para cada ventana de 1 minuto y cada departamento calcula: casos, fallecidos,
casos graves o fallecidos y edad promedio. En cada micro-lote muestra en consola el
ranking de la ventana actual, un resumen nacional y alertas, y actualiza un grafico
PNG y un CSV en la carpeta resultados_streaming/.

Uso:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_covid.py
"""

import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

TOPIC = "covid_casos"
SERVIDOR = "localhost:9092"
CARPETA = "resultados_streaming"
# Un departamento entra en alerta si en una ventana tiene al menos este numero de casos graves o fallecidos
UMBRAL_ALERTA = 2

ESQUEMA = StructType([
    StructField("id_evento", LongType()),
    StructField("departamento", StringType()),
    StructField("edad", IntegerType()),
    StructField("sexo", StringType()),
    StructField("tipo_contagio", StringType()),
    StructField("estado", StringType()),
    StructField("timestamp", LongType()),
])


def procesar_lote(lote_df, lote_id):
    """Se ejecuta en cada micro-lote con el estado completo de las agregaciones."""
    if lote_df.isEmpty():
        return

    # Se cachea para no recalcular el micro-lote en cada accion
    lote_df.persist()
    ultima_ventana = lote_df.agg(F.max("window.start")).collect()[0][0]
    actual = (lote_df.filter(F.col("window.start") == ultima_ventana)
                     .orderBy(F.desc("casos"))
                     .cache())

    inicio = ultima_ventana.strftime("%H:%M")
    fin = actual.first()["window"]["end"].strftime("%H:%M")
    totales = actual.agg(F.sum("casos").alias("casos"), F.sum("fallecidos").alias("fallecidos"),
                         F.sum("graves_o_fallecidos").alias("graves")).collect()[0]

    print("\n" + "=" * 78)
    print(f"Lote {lote_id} | Ventana {inicio} - {fin}")
    print(f"Nacional: {totales['casos']} casos | {totales['graves']} graves o fallecidos | "
          f"{totales['fallecidos']} fallecidos")
    print("=" * 78)
    actual.select("departamento", "casos", "fallecidos", "graves_o_fallecidos",
                  "edad_promedio", "pct_mayores_60").show(10, truncate=False)

    for fila in actual.filter(F.col("graves_o_fallecidos") >= UMBRAL_ALERTA).collect():
        print(f"  ALERTA: {fila['departamento']} registra {fila['graves_o_fallecidos']} "
              f"casos graves o fallecidos en la ventana")

    guardar_tablero(lote_df, actual, inicio, fin)
    actual.unpersist()
    lote_df.unpersist()


def guardar_tablero(lote_df, actual, inicio, fin):
    """Actualiza un CSV con todas las ventanas y un grafico de la ventana actual."""
    try:
        import pandas  # noqa: F401  (lo necesita toPandas)
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return

    os.makedirs(CARPETA, exist_ok=True)
    # La ventana se pasa a texto: toPandas falla con columnas timestamp en algunas versiones de pandas
    (lote_df.select(F.date_format("window.start", "yyyy-MM-dd HH:mm").alias("inicio_ventana"), "departamento", "casos",
                    "fallecidos", "graves_o_fallecidos", "edad_promedio")
            .orderBy("inicio_ventana", F.desc("casos"))
            .toPandas()
            .to_csv(os.path.join(CARPETA, "ventanas.csv"), index=False))

    datos = actual.select("departamento", "casos", "graves_o_fallecidos").limit(15).toPandas().iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(datos["departamento"], datos["casos"], color="tab:blue", label="Casos")
    ax.barh(datos["departamento"], datos["graves_o_fallecidos"], color="tab:red",
            label="Graves o fallecidos")
    ax.set_title(f"Casos de COVID-19 en tiempo real - ventana {inicio} a {fin}")
    ax.set_xlabel("Casos en la ventana")
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(CARPETA, "tablero_tiempo_real.png"), dpi=110)
    plt.close(fig)


def main():
    spark = SparkSession.builder.appName("StreamingCovidColombia").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    crudo = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", SERVIDOR)
                  .option("subscribe", TOPIC)
                  .option("startingOffsets", "latest")
                  .load())

    casos = (crudo.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("d"))
                  .select("d.*")
                  .withColumn("hora_evento", F.col("timestamp").cast("timestamp"))
                  .filter(F.col("departamento").isNotNull()))

    por_ventana = (casos.withWatermark("hora_evento", "1 minute")
                        .groupBy(F.window("hora_evento", "1 minute"), "departamento")
                        .agg(F.count("*").alias("casos"),
                             F.sum(F.when(F.col("estado") == "FALLECIDO", 1).otherwise(0)).alias("fallecidos"),
                             F.sum(F.when(F.col("estado").isin("GRAVE", "FALLECIDO"), 1).otherwise(0))
                              .alias("graves_o_fallecidos"),
                             F.round(F.avg("edad"), 1).alias("edad_promedio"),
                             F.round(F.avg(F.when(F.col("edad") >= 60, 100).otherwise(0)), 1)
                              .alias("pct_mayores_60")))

    consulta = (por_ventana.writeStream
                           .outputMode("complete")
                           .foreachBatch(procesar_lote)
                           .trigger(processingTime="10 seconds")
                           .start())

    print(f"Escuchando el topic '{TOPIC}'... (Ctrl+C para detener)")
    consulta.awaitTermination()


if __name__ == "__main__":
    main()
