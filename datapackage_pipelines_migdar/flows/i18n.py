from fuzzywuzzy import process as process_fw, fuzz
import tabulator
import dataflows as DF

URL = 'https://docs.google.com/spreadsheets/d/1LNc_1TUdF_LWxvT3pqcgMbO_aSuN5QPDkQjeI25QXhg/edit#gid={}'

sources = dict([
    ('languages', 0),
    ('life_areas', 809871679),
    ('source_kind', 355649685),
    ('item_kind', 1907901926),
    ('tags', 1460345471),
    ('regions', 208533187),
    ('org_kind', 767728199),
    ('specialties', 889167589),
    ('provided_services', 689904800),
    ('target_audiences', 639424575),
    ('yes_no', 1242341367),
    ('compact_services', 1440766219),
])


LANGS = ('hebrew', 'english', 'arabic')


def clean_row(row):
    for l in LANGS:
        row[l] = row[l] and row[l].strip()


def clean(x):
    return x.replace('\xa0', ' ').strip().lower()


def extract_values(row):
    values = [
        v.strip()
        for k, v in row.items()
        if k.startswith('value') and v
    ]
    values.extend(
        row[k].strip()
        for k in LANGS
        if row.get(k)
    )
    values = list(set(values))
    dvalues = [
        '{} - {}'.format(v1, v2)
        for v1 in values
        for v2 in values
        if v1 != v2
    ]
    return values + dvalues


translations = {}
for source, gid in sources.items():
    url = URL.format(gid)
    translations[source] = DF.Flow(
        DF.load(url),
        clean_row,
        DF.add_field('values', 'array',
                     default=extract_values),
        DF.filter_rows(lambda row: row['hebrew']),
        DF.select_fields(list(LANGS) + ['values'])
    ).results()[0][0]
    tx = {}
    complained = set()
    for row in translations[source]:
        v = row.get('values')
        if not v:
            continue
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
                if not val_ or len(val) < 3:
                    continue
                best = process_fw.extractBests(val_, tx_keys,
                                               scorer=fuzz.UQRatio, limit=2,
                                               score_cutoff=90)
                if len(best) > 0:
                    if len(best) > 1:
                        if best[0][1] < 100:
                            if tx[best[0][0]] != tx[best[1][0]]:
                                print('POSSIBLE BAD TRANSLATION', field, val_, best)
                    translation = tx[best[0][0]]
                    for col, suffix in zip(COLS, SUFFIXES):
                        key = '{}{}'.format(field, suffix)
                        to_val = translation[col]
                        if to_val:
                            to_val = clean(to_val)
                            if to_val:
                                if to_val not in row[key]:
                                    row[key].append(to_val)
                            else:
                                if val not in row[key]:
                                    row[key].append(val)
                        else:
                            if val not in row[key]:
                                row[key].append(val)
                else:
                    if val_ not in complained:
                        complained.add(val_)
                    for lang in SUFFIXES:
                        key = '{}{}'.format(field, lang)
                        if val not in row[key]:
                            row[key].append(val)
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


def fix_urls(fields):
    def func(row):
        for f in fields:
            v = row.get(f)
            if isinstance(v, str):
                v = v.strip()
                if v.startswith('http'):
                    continue
                row[f] = 'https://' + v
                print('FIXED __https://__{}'.format(v))
    return func
