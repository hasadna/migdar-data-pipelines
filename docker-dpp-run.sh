#!/bin/sh

export DPP_REDIS_HOST=127.0.0.1
export DPP_CELERY_BROKER=redis://localhost:6379/6

if [ "$1" = "server" ]; then
    echo "Starting Server"
    redis-server /etc/redis.conf --daemonize yes --dir /var/redis
    until [ `redis-cli ping | grep -c PONG` = 1 ]; do echo "Waiting 1s for Redis to load"; sleep 1; done
    rm -f /var/run/dpp/dpp-celerybeat.pid /var/run/dpp/dpp-celeryd-management.pid /var/run/dpp/dpp-celeryd-worker.pid
    python /dpp/docker/github_config.py
    dpp init

    echo "Deleting `redis-cli -n 6 KEYS '*' | wc -l` keys"
    redis-cli -n 6 FLUSHDB
    echo "Remaining `redis-cli -n 6 KEYS '*' | wc -l` keys"

    SCHEDULER=1 python3 -m celery -b $DPP_CELERY_BROKER -A datapackage_pipelines.app -l INFO --pidfile=/var/run/dpp/dpp-celerybeat.pid beat &
    python3 -m celery -b $DPP_CELERY_BROKER --concurrency=1 -A datapackage_pipelines.app -Q datapackage-pipelines-management -l INFO --pidfile=/var/run/dpp/dpp-celeryd-management.pid worker &
    python3 -m celery -b $DPP_CELERY_BROKER --concurrency=4 -A datapackage_pipelines.app -Q datapackage-pipelines -l INFO --pidfile=/var/run/dpp/dpp-celeryd-worker.pid worker &
    gunicorn --bind 0.0.0.0:5000 datapackage_pipelines.web:app --workers "${GUNICORN_WORKERS:-2}"
else
    /usr/local/bin/dpp "$@"
fi;
