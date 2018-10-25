import os
from functools import lru_cache
import requests


DATAFLOWS_DB_ENGINE = os.environ.get('DATAFLOWS_DB_ENGINE', 'postgresql://postgres:123456@localhost:15432/postgres')


@lru_cache(maxsize=1)
def get_migdar_session():
    migdar_session = requests.Session()
    migdar_session.auth = (os.environ['MIGDAR_USERNAME'], os.environ['MIGDAR_PASSWORD'])
    return migdar_session
