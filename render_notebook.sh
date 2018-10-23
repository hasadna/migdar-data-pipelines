#!/usr/bin/env bash

( [ -z "${MIGDAR_USERNAME}" ] || [ -z "${MIGDAR_PASSWORD}" ] ) && echo missing required env vars && exit 1

env MIGDAR_USERNAME="${MIGDAR_USERNAME}" MIGDAR_PASSWORD="${MIGDAR_PASSWORD}" \
    jupyter nbconvert --execute "${1}.ipynb" --to notebook --inplace --ExecutePreprocessor.timeout=-1 &&\
    jupyter nbconvert "${1}.ipynb" --to markdown
