FROM frictionlessdata/datapackage-pipelines:2.1.8

RUN echo "http://nl.alpinelinux.org/alpine/v3.11/main" > /etc/apk/repositories && \
    echo "http://nl.alpinelinux.org/alpine/v3.11/community" >> /etc/apk/repositories && \
    echo "http://nl.alpinelinux.org/alpine/v3.11/testing" >> /etc/apk/repositories &&  cat /etc/apk/repositories
RUN apk --update --no-cache add -U musl==1.1.24-r0 libstdc++==9.2.0-r3 
RUN apk --update --no-cache add bash wget nodejs npm nss chromium
RUN npm install -g npm@latest
RUN cd /pipelines/ && PUPPETEER_SKIP_CHROMIUM_DOWNLOAD="true" npm install puppeteer

COPY docker-dpp-run.sh /dpp/docker/run.sh

COPY requirements.txt /pipelines/
RUN python3 -m pip install -Ur requirements.txt
RUN python3 -m pip install -U https://github.com/datahq/dataflows/archive/master.zip

COPY setup.py /pipelines/
RUN python3 -m pip install -e .

ENV DATAFLOWS_ELASTICSEARCH=localhost:19200
ENV CHROME_BIN="/usr/bin/chromium-browser"

COPY datapackage_pipelines_migdar /pipelines/datapackage_pipelines_migdar
COPY download_search_results_unique_records.sh /pipelines/
COPY pipeline-spec.yaml /pipelines/
