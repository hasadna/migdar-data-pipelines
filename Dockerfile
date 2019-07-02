FROM frictionlessdata/datapackage-pipelines:2.1.8

RUN apk --update --no-cache add bash wget

COPY docker-dpp-run.sh /dpp/docker/run.sh

COPY requirements.txt /pipelines/
RUN python3 -m pip install -Ur requirements.txt

COPY setup.py /pipelines/
RUN python3 -m pip install -e .

ENV DPP_ELASTICSEARCH=localhost:19200

COPY datapackage_pipelines_migdar /pipelines/datapackage_pipelines_migdar
COPY download_search_results_unique_records.sh /pipelines/
COPY pipeline-spec.yaml /pipelines/
