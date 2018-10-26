#!/usr/bin/env bash

LIMIT_ROWS="${1:--0}"

echo LIMIT_ROWS = "${LIMIT_ROWS}"

if [ -e data/search_results/unique_records.csv ]; then
    echo data already exists, delete to recreate
else
    mkdir -p data/search_results
    wget -qO /dev/stdout \
         --http-user=${MIGDAR_USERNAME} --http-password=${MIGDAR_PASSWORD} \
         https://migdar-internal-search.odata.org.il/__data/search_results/unique_records.csv \
    | head -n${LIMIT_ROWS} \
    > data/search_results/unique_records.csv
fi && echo $(cat data/search_results/unique_records.csv | wc -l) rows
