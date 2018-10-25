#!/usr/bin/env bash

LIMIT_ROWS="${1:--0}"

mkdir -p data/search_results
wget -qO /dev/stdout \
     --http-user=${MIGDAR_USERNAME} --http-password=${MIGDAR_PASSWORD} \
     https://migdar-internal-search.odata.org.il/__data/search_results/unique_records.csv \
| head -n${LIMIT_ROWS} \
> data/search_results/unique_records.csv &&\
cat data/search_results/unique_records.csv | wc -l
