import os
import re
from functools import lru_cache
import requests


DATAFLOWS_DB_ENGINE = os.environ.get('DATAFLOWS_DB_ENGINE', 'postgresql://postgres:123456@localhost:15432/postgres')


@lru_cache(maxsize=1)
def get_migdar_session():
    migdar_session = requests.Session()
    migdar_session.auth = (os.environ['MIGDAR_USERNAME'], os.environ['MIGDAR_PASSWORD'])
    return migdar_session


link = re.compile('(http[s]?://[-_?&A-Z0-9a-z./=]+)', re.MULTILINE)

def fix_links(field):
    def func(row):
        if row.get(field):
            row[field] = link.sub('<a href="\\1" target="_blank">\\1</a>', row[field])
    return func


if __name__ == '__main__':
    data = {
        'foo': 'http://google.com/a.c.d fdsfsdf s sdf sdf https://mysite123.www.com/a/path/TO/FileFName.xlsx?a=2&b=3 bla'
    }
    fix_links('foo')(data)
    print(data)