"""
Productor Kafka: simula la llegada en tiempo real de reportes de casos de COVID-19.

Los eventos son sinteticos, pero sus proporciones se tomaron del dataset historico
del INS: la probabilidad de cada departamento es proporcional a sus casos reales y
la probabilidad de fallecer depende del grupo de edad (letalidad observada en batch).

Uso:
    python3 kafka_producer_covid.py              # 2 eventos por segundo
    python3 kafka_producer_covid.py 5            # 5 eventos por segundo
"""

import json
import random
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "covid_casos"
SERVIDOR = "localhost:9092"

# Casos historicos por departamento (miles), distritos sumados a su departamento
DEPARTAMENTOS = {
    "BOGOTA": 1888, "ANTIOQUIA": 955, "VALLE": 573, "ATLANTICO": 419, "CUNDINAMARCA": 331,
    "SANTANDER": 298, "BOLIVAR": 204, "BOYACA": 131, "TOLIMA": 128, "NORTE SANTANDER": 126,
    "CORDOBA": 123, "CALDAS": 120, "MAGDALENA": 118, "RISARALDA": 110, "CESAR": 110,
    "NARIÑO": 109, "META": 108, "HUILA": 104, "CAUCA": 77, "QUINDIO": 73, "SUCRE": 67,
    "GUAJIRA": 58, "CASANARE": 43, "CAQUETA": 26, "PUTUMAYO": 22, "CHOCO": 19, "ARAUCA": 17,
    "SAN ANDRES": 10, "AMAZONAS": 8, "GUAVIARE": 6, "VICHADA": 3, "GUAINIA": 3, "VAUPES": 2,
}

# (edad minima, edad maxima, peso en los casos, probabilidad de fallecer)
GRUPOS_EDAD = [
    (0, 9, 5, 0.001), (10, 19, 9, 0.0005), (20, 29, 22, 0.001), (30, 39, 22, 0.003),
    (40, 49, 16, 0.009), (50, 59, 13, 0.027), (60, 69, 7, 0.07), (70, 79, 4, 0.16),
    (80, 100, 2, 0.30),
]

TIPOS_CONTAGIO = ["COMUNITARIA", "RELACIONADO", "IMPORTADO"]
PESOS_CONTAGIO = [70, 29, 1]


def generar_caso(id_evento):
    edad_min, edad_max, _, prob_fallecer = random.choices(
        GRUPOS_EDAD, weights=[g[2] for g in GRUPOS_EDAD])[0]
    azar = random.random()
    if azar < prob_fallecer:
        estado = "FALLECIDO"
    elif azar < prob_fallecer * 1.5:
        estado = "GRAVE"
    elif azar < prob_fallecer * 3:
        estado = "MODERADO"
    else:
        estado = "LEVE"

    return {
        "id_evento": id_evento,
        "departamento": random.choices(list(DEPARTAMENTOS), weights=DEPARTAMENTOS.values())[0],
        "edad": random.randint(edad_min, edad_max),
        "sexo": random.choice(["F", "M"]),
        "tipo_contagio": random.choices(TIPOS_CONTAGIO, weights=PESOS_CONTAGIO)[0],
        "estado": estado,
        "timestamp": int(time.time()),
    }


def main():
    eventos_por_segundo = float(sys.argv[1]) if len(sys.argv) > 1 else 2
    productor = KafkaProducer(
        bootstrap_servers=[SERVIDOR],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
    )

    print(f"Enviando casos simulados al topic '{TOPIC}' ({eventos_por_segundo} por segundo). Ctrl+C para detener.")
    id_evento = 1
    try:
        while True:
            caso = generar_caso(id_evento)
            productor.send(TOPIC, value=caso)
            hora = datetime.fromtimestamp(caso["timestamp"]).strftime("%H:%M:%S")
            print(f"[{hora}] #{id_evento} {caso['departamento']:<16} edad={caso['edad']:<3} "
                  f"sexo={caso['sexo']} estado={caso['estado']}")
            id_evento += 1
            time.sleep(1 / eventos_por_segundo)
    except KeyboardInterrupt:
        print("\nProductor detenido.")
    finally:
        productor.flush()
        productor.close()


if __name__ == "__main__":
    main()
