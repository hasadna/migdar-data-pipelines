import dataflows as DF
import re
import requests

RE = '(http[s]?://[-_?&A-Z0-9a-z./=%]+)'
RE = re.compile(RE)

configuration = [
    dict(
        name='publications',
        filename='publications',
        title='page_title',
    ),
    dict(
        name='orgs',
        filename='orgs',
        title='org_name',
    ),
    dict(
        name='datasets',
        filename='out',
        title='chart_title',
    )
]

URL_TEMPLATE='data/{name}_in_es/data/{filename}.csv'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:86.0) Gecko/20100101 Firefox/86.0',    
}


def unwind():
    def func(rows):
        for row in rows:
            for url in set(row['urls']):
                row['url'] = url
                yield row
    return func


def check_broken():
    def func(rows):
        for row in rows:
            error = None
            try:
                resp = requests.head(row['url'], allow_redirects=True, headers=HEADERS)
                if resp.status_code >= 300:
                    error = '%s: %s' % (resp.status_code, resp.reason)
            except requests.exceptions.RequestException as e:
                error = str(e.__class__.__name__)
            if error:
                print(row['url'], error)
                row['error'] = error
                yield row
    return func

def get_title(title_field):
    def wrapper(title_field_):
        def func(r):
            if title_field_ not in r:
                print('ERRRR, missing field %s in %r' % (title_field_, r))
            return r[title_field_]
        return func
    return wrapper(title_field)

def broken_links_flow():
    return DF.Flow(
        *[
            DF.Flow(
                DF.load(URL_TEMPLATE.format(**c), name=c['name']),
                DF.add_field('__name', 'string', c['name'], resources=c['name']),
                DF.add_field('__title', 'string', get_title(c['title']), resources=c['name']),
            )
            for c in configuration
        ],
        DF.add_field('urls', 'array', lambda r: RE.findall(str(r))),
        DF.add_field('link', 'string', lambda r: 'https://yodaat.org/item/{doc_id}'.format(**r)),
        DF.concatenate(dict(
            name=['__name'],
            title=['__title'],
            link=[],
            urls=[],
        )),
        DF.add_field('url', 'string'),
        DF.add_field('error', 'string'),
        unwind(),
        DF.delete_fields(['urls']),
        check_broken(),
    )

def flow(*_):
    return DF.Flow(
        broken_links_flow(),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.printer(),
        DF.dump_to_path('data/broken_links')
    )

if __name__ == '__main__':
    DF.Flow(
        broken_links_flow(),
        DF.printer()        
    ).process()