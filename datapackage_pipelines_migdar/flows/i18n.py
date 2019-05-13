from fuzzywuzzy import process as process_fw
import tabulator
import dataflows as DF

URL = 'https://docs.google.com/spreadsheets/d/1LNc_1TUdF_LWxvT3pqcgMbO_aSuN5QPDkQjeI25QXhg/edit#gid={}'

sources = dict([
    ('languages', 0),
    ('life_areas', 809871679),
    ('source_kind', 355649685),
    ('resource_type', 1907901926),
    ('tags', 1460345471),
    ('regions', 208533187),
    ('org_kind', 767728199),
    ('specialties', 889167589),
    ('provided_services', 689904800),
    ('target_audiences', 639424575),
    ('yes_no', 1242341367),
])


def clean_row(row):
    row['hebrew'] = row['hebrew'] and row['hebrew'].strip()
    row['english'] = row['english'] and row['english'].strip()
    row['arabic'] = row['arabic'] and row['arabic'].strip()


def clean(x):
    return x.replace('\xa0', ' ').strip().lower()


translations = {}
for source, gid in sources.items():
    url = URL.format(gid)
    translations[source] = DF.Flow(
        DF.load(url),
        DF.add_field('values', 'array',
                     default=lambda row: [
                         v.strip()
                         for k, v in row.items()
                         if k.startswith('value') and v
                     ]),
        DF.filter_rows(lambda row: row['hebrew']),
        clean_row,
        DF.delete_fields(['value\\d'])
    ).results()[0][0]
    tx = {}
    complained = set()
    for row in translations[source]:
        for v in row.values():
            if not v:
                continue
            if isinstance(v, str):
                v = clean(v)
                if tx.get(v) not in (None, row):
                    if v not in complained:
                        complained.add(v)
                tx[v] = row
            else:
                for vv in v:
                    vv = clean(vv)
                    if tx.get(vv) not in (None, row):
                        if vv not in complained:
                            complained.add(vv)
                    tx[vv] = row
    if len(complained) > 0:
        print('{}:'.format(source))
        for i in sorted(complained):
            print('- {}'.format(i))
    translations[source] = tx


def split_and_translate(field, translations_key, delimiter=None, keyword=False):

    SUFFIXES = ['', '__en', '__ar']
    COLS = ['hebrew', 'english', 'arabic']

    tx = translations[translations_key]
    tx_keys = list(tx.keys())
   
    complained = set()

    def process(rows):
        for row in rows:
            values = row.pop(field) or ''
            if isinstance(values, str):
                if delimiter:
                    values = values.split(delimiter)
                else:
                    values = [values]
            for lang in SUFFIXES:
                row['{}{}'.format(field, lang)] = []
            for val in values:
                val_ = clean(val)
                if not val_:
                    continue
                best = process_fw.extractBests(val_, tx_keys, limit=2, score_cutoff=80)
                if len(best) > 0:
                    if len(best) > 1:
                        if best[0][1] < 100:
                            if tx[best[0][0]] != tx[best[1][0]]:
                                print('POSSIBLE BAD TRANSLATION', field, val_, best)
                    translation = tx[best[0][0]]
                    for col, suffix in zip(COLS, SUFFIXES):
                        to_val = translation[col]
                        if to_val:
                            to_val = clean(to_val)
                            if to_val:
                                row['{}{}'.format(field, suffix)].append(to_val)
                            else:
                                row['{}{}'.format(field, suffix)].append(val)
                        else:
                            row['{}{}'.format(field, suffix)].append(val)
                else:
                    if val_ not in complained:
                        complained.add(val_)
                    for lang in SUFFIXES:
                        row['{}{}'.format(field, lang)].append(val)
            yield row
        if len(complained) > 0:
            print('COMPLAINTS FOR', field)
            for x in sorted(complained):
                print('\t{!r}'.format(x))

    def func(package):
        fields = package.pkg.descriptor['resources'][0]['schema']['fields']
        fields = list(filter(lambda x: x['name'] != field, fields))
        fields.extend([
            {
                'name': '{}{}'.format(field, suffix),
                'type': 'array',
                'es:itemType': 'string',
                'es:keyword': keyword,
            }
            for suffix in SUFFIXES
        ])
        package.pkg.descriptor['resources'][0]['schema']['fields'] = fields
        yield package.pkg
        for res in package:
            yield process(res)
    return func
