#!/bin/bash
# Descarga el dataset "Casos positivos de COVID-19 en Colombia" (INS - datos.gov.co).
#
# Uso:
#   bash descargar_datos.sh            -> dataset completo (~6,4 millones de filas, mas de 1 GB)
#   bash descargar_datos.sh 500000     -> muestra de N filas desde la API (para pruebas rapidas)

set -e
mkdir -p data
ARCHIVO="data/casos_covid_colombia.csv"

if [ -z "$1" ]; then
    echo "Descargando el dataset completo (puede tardar varios minutos)..."
    wget -O "$ARCHIVO" "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"
else
    echo "Descargando una muestra de $1 filas..."
    wget -O "$ARCHIVO" "https://www.datos.gov.co/resource/gt2j-8ykr.csv?\$limit=$1"
fi

echo "Archivo guardado en $ARCHIVO ($(wc -l < "$ARCHIVO") lineas)"
