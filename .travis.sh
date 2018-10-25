if [ "${1}" == "install" ]; then
    python3 -m pip install \
        jupyter jupyterlab ipython \
        plyvel psycopg2 datapackage-pipelines-elasticsearch \
        'https://github.com/OriHoch/dataflows/archive/specify-encoding-for-load.zip#egg=dataflows[speedup]' \
        'https://github.com/frictionlessdata/datapackage-pipelines/archive/2.0.0.zip#egg=datapackage-pipelines[speedup]' &&\
    python3 -m pip install -e .

elif [ "${1}" == "script" ]; then
    ./render_notebook.sh QUICKSTART

elif [ "${1}" == "deploy" ]; then
    travis_ci_operator.sh github-update self master "
        cp -f $PWD/QUICKSTART.md $PWD/QUICKSTART.ipynb ./ &&\
        git add QUICKSTART.md QUICKSTART.ipynb
    " "update QUICKSTART notebook"

fi
