import requests
import dataflows as DF


URL = 'https://api.zotero.org/groups/2095819/items?' + \
    'key=l5A0BScYGE0eEVKBP1IwYfYR&limit=100&start={}'
session = requests.Session()


def get(start=0):
    start = 0
    while True:
        results = session.get(URL.format(start)).json()
        for i, res in enumerate(results):
            for creator in res['data'].get('creators', []):
                if 'firstName' not in creator and 'name' not in creator:
                    print(creator)
            yield res['data']
        if len(results) < 100:
            break
        start += 100


def simplify_tags(row):
    row['tags'] = [
        x['tag']
        for x in row['tags']
    ]


def extract_tags(field='tags', prefixes=None):
    if prefixes is not None:
        def remove_prefix(row):
            row['tags'] = [
                x
                for x in row['tags']
                if all(not x.startswith('{}_'.format(prefix)) for prefix in prefixes)
            ]

        def collect(rows):
            options = set()
            for row in rows:
                options.update(row[field])
                yield row
            print('OPTIONS FOR {}: {}'.format(field, sorted(options)))

        return DF.Flow(
            DF.add_field(field, 'array',
                         lambda row: [
                             t.split('_', 1)[1]
                             for t in row['tags']
                             if any(t.startswith('{}_'.format(prefix)) for prefix in prefixes)
                         ]),
            remove_prefix,
            collect
        )
    else:
        def verify_tags(row):
            for tag in row[field]:
                if '_' in tag:
                    print('Found prefix: {}'.format(tag))

        return DF.Flow(
            verify_tags,
        )


def flow(*args):
    return DF.Flow(
        get(),
        simplify_tags,
        extract_tags('life_areas', ['Domain']),
        extract_tags('source_kind', ['Source', 'Resource', 'Resouce']),
        DF.add_field('author', 'string',
            lambda r: None if not r.get('creators') else ', '.join(
                (
                    '{name}'.format(**c)
                    if 'name' in c else
                    '{firstName} {lastName}'.format(**c)
                )
                for c in r.get('creators', [])
                if c.get('creatorType') == 'author'
            )
        ),
        DF.concatenate(
            dict(
                title=[],
                pubyear=['date'],
                publisher=['institution', 'publication'],
                author=[],
                life_areas=[],
                notes=['abstractNote'],
                languages=[],
                tags=[],
                url=[],
                migdar_id=['key'],
                item_kind=['reportType'],
                source_kind=[],
            ),
            target={'name': 'zotero', 'path': 'zotero.csv'}
        ),
        DF.dump_to_path('data/zotero'),
        DF.update_resource(None, **{'dpp:streaming': True})
    )


if __name__ == '__main__':
    res, _, _ = DF.Flow(
        flow(),
        # DF.printer(num_rows=1)
    ).results()
    import pprint
    # pprint.pprint(sorted(res[0][0].keys()))
    # pprint.pprint(res[0][:10], width=120)