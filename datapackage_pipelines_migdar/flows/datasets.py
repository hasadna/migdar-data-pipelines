#!/usr/bin/env python
# coding: utf-8
import tabulator
import dataflows as DF
from hashlib import md5
from decimal import Decimal
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.i18n import split_and_translate, fix_urls
from datapackage_pipelines_migdar.flows.constants import REVISION

URLS = [
    [
        # Gender Statistics
        'https://docs.google.com/spreadsheets/d/1uDZ-aPGie30IHaCqJOYgERl9hyVCKDm62TrBgkF3jgo/view#gid=',
        [
            '1012201957',
            '1073916128',
            '1134335030',
            '1153286427',
            '1228415763',
            '1509914874',
            '1619687497',
            '1648779124',
            '1702779521',
            '1938181021',
            '377560845',
            '479978628',
            '520697029',
            '59161098',
            '723318014',
            '885890663',
            '942294406',
        ]
    ],
    [
        # Gender Index
        'https://docs.google.com/spreadsheets/d/1lgWPjyLflobl-KZKAlZieIVdyrFw2Q6q5Jf45e155Nw/view#gid=',
        [
            '1012229604',
            '1131352277',
            '1367638582',
            '1427627014',
            '1643825489',
            '1701169040',
            '1981367694',
            '2025391121',
            '2118420400',
            '266256601',
            '340008249',
            '424310605',
            '563852429',
        ]
    ]
]


sheets = [base + gid for base, gids in URLS for gid in gids]
all_headers = set()


def transpose(sheet):
    stream = tabulator.Stream(sheet).open()
    cells = list(stream.iter())
    num_rows = len(cells)
    headers = None
    print(num_rows)
    for i in range(num_rows):
        row = [(cells_row[i] if len(cells_row) > i else None) for cells_row in cells]
        if any(row):
            if i == 0:
                headers = row
                all_headers.update(headers) 
            else:
                yield dict(zip(headers, row))
        else:
            break


def set_defaults(row):
    row['series_title'] = row.get('series_title') or row['gender']
    for x in ['title', 'abstract']:
        for lang in ['', '__ar']:
            f = 'chart_{}{}'.format(x, lang)
            row[f] = row.get(f) or row.get('series_{}{}'.format(x, lang))


def extrapulate_years(row):
    ey = row['extrapulation_years']
    out = []
    if ey:
        parts = ey.split(',')
        for part in parts:
            if '-' in part:
                year_range = part.split('-')
                out.extend(range(int(year_range[0]), int(year_range[1])+1))
            else:
                out.append(int(part))
        out = [str(x) for x in sorted(out)]
    row['extrapulation_years'] = out


def fix_values(rows):
    for row in rows:
        if not row.get('chart_title'):
            print('PROBABLY BAD LINE %r' % row)
            continue
        if row.get('value'):
            row['value'] = row['value'].replace('%', '')
            yield row


def fix_units(row):
    assert row['units'] in (
        'אחוזים עד 1', 'אחוזים עד 100', 'מספר', 'ש"ח', 'שנים', 'מספר עד 1'
    ), 'BAD UNITS ' + repr(row)
    if row['units'] == 'אחוזים עד 1':
        assert 0 <= row['value'] <= 1
        row['value'] *= 100
        row['units'] = 'אחוזים עד 100'


def verify_percents(row):
    if row['units'] != 'מספר עד 1' and all(0 <= x <= 1 for x in row['value'] if isinstance(x, Decimal)):
        print('BAD UNITS (PROBABLY):')
        print(row['kind'])
        print(row['chart_title'])
        print(row['series_title'])
        print(row['units'])
        print(row['value'])
    if row['units'] == 'מספר עד 1' and not all(0 <= x <= 1 for x in row['value'] if isinstance(x, Decimal)):
        print('BAD UNITS (PROBABLY 2):')
        print(row['kind'])
        print(row['chart_title'])
        print(row['series_title'])
        print(row['units'])
        print(row['value'])


CHART_FIELDS = [
    'kind', 'gender_index_dimension', 'life_areas', 'item_type', 'tags', 'language',
    'author', 'author__ar', 'author__en',
    'institution', 'institution__ar', 'institution__en',
    'chart_title', 'chart_title__ar', 'chart_title__en',
    'chart_abstract', 'chart_abstract__ar', 'chart_abstract__en',
    'last_updated_at', 'chart_type', 'full_data_source'
]

SERIES_FIELDS = [
    'series_title', 'series_title__ar', 'series_title__en',
    'series_abstract', 'series_abstract__ar', 'series_abstract__en',
    'source_description', 'source_description__ar', 'source_description__en',
    'source_detail_description',
    'gender', 'gender__ar', 'gender__en',
    'extrapulation_years',
    'source_url', 'units', 'order_index',
]

FIELD_MAPPING = dict(
    kind=['אזור באתר:'],
    gender_index_dimension=['ממד במדד המגדר'],
    life_area1=['תחום חיים1 ביודעת'],
    life_area2=['תחום חיים2 ביודעת'],
    life_area3=['תחום חיים3 ביודעת'],
    author=['Author'],
    author__ar=['מחברת בערבית'],
    author__en=['מחברת באנגלית'],
    institution=['Institution'],
    institution__ar=['מוסד בערבית'],
    institution__en=['מוסד באנגלית'],
    item_type=['Item type'],
    tags=['Tags'],
    language=[],
    chart_title=['כותרת התרשים (נשים וגברים ביחד):', 'כותרת התרשים בעברית'],
    chart_title__ar=['כותרת התרשים בערבית'],
    chart_title__en=['כותרת התרשים באנגלית'],
    chart_abstract=['אבסטרקט של התרשים', 'אבסטרקט בעברית'],
    chart_abstract__ar=['אבסטרקט התרשים בערבית', 'אבסטרקט בערבית'],
    chart_abstract__en=['אבסטרקט התרשים באנגלית', 'אבסטרקט באנגלית'],
    series_title=['כותרת סדרת הנתונים (נשים או גברים):'],
    series_title__ar=['כותרת הסידרה בערבית'],
    series_title__en=['כותרת הסידרה באנגלית'],
    series_abstract=[
        'אבסטרקט של סדרת הנתונים (נשים או גברים)',            
    ],
    series_abstract__ar=['אבסטרקט הסידרה בערבית'],
    series_abstract__en=['אבסטרקט הסידרה באנגלית'],
    source_description=[
        'מקור הנתונים',
        'מקור הנתונים שיופיע מתחת לתרשים',
        'מקור הנתונים בעברית',
    ],
    source_description__ar=['מקור הנתונים בערבית'],
    source_description__en=['מקור הנתונים באנגלית', 'מקור הנתונים  באנגלית'],
    source_detail_description=[
        'מקור הנתונים - כותרת הלוח',
        'פירוט נוסף על מקור הנתונים (רלבנטי רק כאשר אין לינק למקור הנתונים)'
    ],
    source_url=[
        'לינק למקור הנתונים',
        'מקור הנתונים - לינק:',
        'קישור למקור הנתונים',
    ],
    full_data_source=[
        'קישור לקובץ הנתונים המלא ביודעת'
    ],
    gender=['מגדר', 'מגדר:', 'שם הסדרה', 'שם הסידרה', 'שם הסידרה:', 'שם הסידרה בעברית'],
    gender__ar=['שם הסידרה בערבית', 'מגדר בתרגום לערבית', 'מגדר בערבית'],
    gender__en=['שם הסידרה באנגלית', 'מגדר בתרגום לאנגלית', 'מגדר באנגלית'],
    units=['יחידות'],
    extrapulation_years=[
        'שנת אקסטרפולציה (אם קיימת, מהשנה שבה עושות אקסטרפולציה):',
        'שנת אקסטרפולציה (טווח שנים או שנה ספציפית, או שנת התחלה):',
        'שנת אקסטרפולציה (טווח שנים או שנת התחלה):',            
    ],
    chart_type=['סוג התרשים', 'סוג תרשים'],
    last_updated_at=['תאריך עדכון אחרון'],
    year=[],
    value=[],
)


def verify_unused_fields():
    fields = []
    IGNORED = [
        'צבע קו התרשים',
        'יחידת המדידה',
    ]
    for k, v in FIELD_MAPPING.items():
        fields.extend(v)
        fields.append(k)

    def func(rows):
        for i, row in enumerate(rows):
            if i == 0:
                for k in row.keys():
                    if k not in fields and k not in IGNORED:
                        print('UNUSED FIELD in %s: %s' % (rows.res.name, k))
            yield row

    return func

def fix_languages():
    def func(row):
        row['language'] = 'heb,eng,ara'
    return func


def verify_chart_types():
    def func(row):
        row['chart_type'] = {
            'ברים נערמים של הזיקות השונות, לפי שנים': 'stacked',
            'ברים נערמים, לפי שנים': 'stacked',
            'ברים (אופקיים או אנכיים), שנים בתוך תחומי לימוד': 'hbars',
            'ברים נערמים של גברים ונשים, לפי שנים (משמאל לימין)': 'stacked',
            'גברים-נשים רגיל': 'line',
            'תרשים אופקי המשווה בין המדינות, מהערך הגבוה ביותר לערך הנמוך ביותר': 'hbars',
            'ברים מלמעלה למטה, שנים בתוך מדינות': 'hbars',
            'ברים מוערמים, לפי שנים': 'stacked',
            'ברים נערמים לפי שנים': 'stacked',
            'ברים נערמים': 'stacked',
            'ברים': 'stacked',
            None: 'line',
        }[row['chart_type']]
    return func


def ensure_chart_title():
    def func(rows):
        chart_title = None
        for row in rows:
            row['chart_title'] = chart_title = row['chart_title'] or chart_title
            yield row
    return func


datasets_flow = DF.Flow(*[
        transpose(sheet)
        for sheet in sheets
    ],
    DF.unpivot(
        [{'name': '([0-9/]+.+)', 'keys': {'year': '\\1'}}],
        [{'name': 'year', 'type': 'string'}],
        {'name': 'value', 'type': 'number'},
    ),
    verify_unused_fields(),
    DF.concatenate(FIELD_MAPPING, target=dict(name='out')),
    fix_urls(['source_url']),
    ensure_chart_title(),
    fix_languages(),
    DF.add_field('order_index', 'integer'),
    lambda rows: ({**row, **{'order_index': i}} for i, row in enumerate(rows)),
    set_defaults,
    extrapulate_years,
    fix_values,
    DF.set_type('value', groupChar=',', bareNumber=True),
    fix_units,
    DF.set_type('extrapulation_years', type='array', **{'es:itemType': 'string'}),
    DF.validate(),
    DF.add_computed_field([
        dict(target=dict(
                name='life_areas',
                type='array',
                **{
                    'es:itemType': 'string',
                    'es:keyword': True
                }
             ),
             operation=lambda row: [x for x in [row.get('life_area{}'.format(i)) 
                                            for i in range(1, 4)]
                                    if x is not None]
            )
    ]),
    DF.delete_fields(['life_area{}'.format(i) for i in range(1, 4)]),
    DF.join_self('out', ['chart_title', 'series_title'], 'out',
        dict([
                (k, None)
                for k in CHART_FIELDS + SERIES_FIELDS
             ] + [
                (k, dict(aggregate='array'))
                for k in [
                    'year', 'value'
                ]
             ]
            )
    ),
    verify_percents,
    DF.add_computed_field([
        dict(target=dict(
                name='dataset',
                type='array'
             ),
             operation=lambda row: list(
                 dict(x=x, y=float(y), q=(x in row['extrapulation_years']))
                 for x,y in zip(row['year'], row['value'])
                 if isinstance(y, Decimal)
             )
            )        
    ]),
    DF.delete_fields(['year', 'value', 'extrapulation_years']),
    DF.join_self('out', ['chart_title'], 'out',
                 dict(
                    [
                        (k, None)
                        for k in CHART_FIELDS
                    ] + [
                        (k, dict(aggregate='array'))
                        for k in SERIES_FIELDS + ['dataset']
                    ] + [
                        ('num_datasets', dict(aggregate='count'))
                    ]
                 )),
    DF.add_computed_field(
        target=dict(
            name='series',
            type='array',
            **{
                'es:itemType': 'object',
                'es:index': False
            }
        ),
        operation=lambda row: sorted(
            (
                dict(
                    (k, row[k][i])
                    for k in SERIES_FIELDS + ['dataset']
                    if len(row[k]) == row['num_datasets']
                )
                for i in range(row['num_datasets'])
            ), key=lambda row: row.get('order_index')
        )
    ),
    DF.delete_fields(SERIES_FIELDS + ['dataset']),
    verify_chart_types(),
    split_and_translate('tags', 'tags', delimiter=',', keyword=True),
    split_and_translate('life_areas', 'life_areas', delimiter=',', keyword=True),
    split_and_translate('language', 'languages', delimiter=',', keyword=True),
    DF.add_computed_field(
        target=dict(name='doc_id', type='string'),
        operation=lambda row: (
            'dataset/' +
            md5(row['chart_title'].encode('utf8')).hexdigest()[:16]
        )
    ),
    *[
        DF.set_type(f, **{'es:keyword': True})
        for f in ['item_type', 'kind', 'language']
    ],
    DF.set_primary_key(['doc_id']),
    *[
        DF.set_type(f, **{'es:title': True})
        for f in [
            'chart_title', 'chart_title__ar', 'chart_title__en',
        ]
    ],
    DF.add_field('title_kw', 'string',
                 default=lambda row: row.get('chart_title'),
                 **{'es:keyword': True}),
    DF.validate(),
    DF.update_resource(resources=None, name='datasets'),
)


def flow(*_):
    return DF.Flow(
        datasets_flow,
        es_dumper('datasets', REVISION, 'datasets_in_es')
    )



if __name__ == '__main__':
    DF.Flow(datasets_flow).process()
