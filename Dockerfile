FROM frictionlessdata/datapackage-pipelines:2.1.8

RUN apk --update --no-cache add bash wget nodejs npm nss chromium
RUN npm install -g npm@latest
RUN cd /pipelines/ && PUPPETEER_SKIP_CHROMIUM_DOWNLOAD="true" npm install puppeteer

COPY docker-dpp-run.sh /dpp/docker/run.sh

COPY requirements.txt /pipelines/
RUN python3 -m pip install -Ur requirements.txt
RUN python3 -m pip install -U https://github.com/datahq/dataflows/archive/master.zip

COPY setup.py /pipelines/
RUN python3 -m pip install -e .

ENV DPP_ELASTICSEARCH=localhost:19200
ENV CHROME_BIN="/usr/bin/chromium-browser"

COPY datapackage_pipelines_migdar /pipelines/datapackage_pipelines_migdar
COPY download_search_results_unique_records.sh /pipelines/
COPY pipeline-spec.yaml /pipelines/
