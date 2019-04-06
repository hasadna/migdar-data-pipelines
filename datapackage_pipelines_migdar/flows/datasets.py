#!/usr/bin/env python
# coding: utf-8
import tabulator
import dataflows as DF
from hashlib import md5
from decimal import Decimal
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper

URLS = [
    ['https://docs.google.com/spreadsheets/d/1uDZ-aPGie30IHaCqJOYgERl9hyVCKDm62TrBgkF3jgo/view#gid=',
     [
         '1648779124',
         '1619687497',
         '1938181021',
         '52a0697029',
         '1509914874',
         '1073916128',
#          '59161098',
#          '1012201957',
     ]
    ],
    ['https://docs.google.com/spreadsheets/d/1lgWPjyLflobl-KZKAlZieIVdyrFw2Q6q5Jf45e155Nw/view#gid=',
     [
         '1643825489',
         '1427627014',
         '266256601',
         '1012229604',
         '563852429',
         '340008249',
         '2025391121',
         '424310605',
         '1981367694',
         '1131352277',
         '2118420400',
         '1701169040',
         '1367638582',         
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
        row = [cells_row[i] if len(cells_row) > i else None for cells_row in cells]
        if any(row):
            if i == 0:
                headers = row
                all_headers.update(headers) 
            else:
                yield dict(zip(headers, row))
        else:
            break

def set_defaults(row):
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

def decimals_to_floats(row):
    row['value'] = float(row['value']) if isinstance(row.get('value'), Decimal) else row.get('value')

CHART_FIELDS = [
    'kind', 'gender_index_dimension', 'life_areas', 'author', 'institution', 'item_type', 'tags', 'language',
    'chart_title', 'chart_title__ar', 'chart_abstract', 'chart_abstract__ar',    
]
SERIES_FIELDS = [
    'series_title', 'series_title__ar', 'series_abstract', 'series_abstract__ar',
    'source_description', 'source_detail_description', 'gender', 'extrapulation_years', 'source_url', 'units',
]

dataets_flow = DF.Flow(*[
        transpose(sheet)
        for sheet in sheets
    ],
    DF.unpivot(
        [{'name': '([0-9/]+)', 'keys': {'year': '\\1'}}],
        [{'name': 'year', 'type': 'string'}],
        {'name': 'value', 'type': 'number'},
    ),
    DF.concatenate(dict(
        kind=['אזור באתר:',],
        gender_index_dimension=['ממד במדד המגדר'],
        life_area1=['תחום חיים1 ביודעת'],
        life_area2=['תחום חיים2 ביודעת'],
        life_area3=['תחום חיים3 ביודעת'],
        author=['Author'],
        institution=['Institution'],
        item_type=['Item type'],
        tags_str=['Tags'],
        language=[],
        chart_title=['כותרת התרשים (נשים וגברים ביחד):', ],
        chart_title__ar=['כותרת התרשים בערבית'],
        chart_abstract=['אבסטרקט של התרשים'],
        chart_abstract__ar=['אבסטרקט התרשים בערבית'],
        series_title=['כותרת סדרת הנתונים (נשים או גברים):'],
        series_title__ar=['כותרת הסידרה בערבית',],
        series_abstract=[
          'אבסטרקט של סדרת הנתונים (נשים או גברים)',            
        ],
        series_abstract__ar=['אבסטרקט הסידרה בערבית'],
        source_description=[
            'מקור הנתונים',
            'מקור הנתונים שיופיע מתחת לתרשים',
        ],
        source_detail_description=[
            'מקור הנתונים - כותרת הלוח',
            'פירוט נוסף על מקור הנתונים (רלבנטי רק כאשר אין לינק למקור הנתונים)',
        ],
        source_url=[
            'לינק למקור הנתונים',
            'מקור הנתונים - לינק:',
        ],
        gender=['מגדר','מגדר:',],
        units=['יחידות',],
        extrapulation_years=[
         'שנת אקסטרפולציה (אם קיימת, מהשנה שבה עושות אקסטרפולציה):',
         'שנת אקסטרפולציה (טווח שנים או שנה ספציפית, או שנת התחלה):',
         'שנת אקסטרפולציה (טווח שנים או שנת התחלה):',            
        ],
        year=[],
        value=[],
    ), target=dict(name='out')),
    set_defaults,
    extrapulate_years,
    DF.set_type('value', groupChar=',', bareNumber=False),
    decimals_to_floats,
    DF.set_type('extrapulation_years', type='array', **{'es:itemType': 'string'}),
    DF.validate(),
    DF.add_computed_field([
        dict(target=dict(
                name='tags',
                type='array',
                **{
                    'es:itemType': 'string',
                    'es:keyword': True
                }
             ),
             operation=lambda row: row['tags_str'].split(',') if row.get('tags_str') else []
            )
    ]),
    DF.delete_fields(['tags_str']),
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
    DF.add_computed_field([
        dict(target=dict(
                name='dataset',
                type='array'
             ),
             operation=lambda row: list(
                 dict(x=x, y=y, q=(x in row['extrapulation_years']))
                 for x,y in zip(row['year'], row['value'])
             )
            )        
    ]),
    DF.delete_fields(['year', 'value', 'extrapulation_years']),
    DF.join_self('out', ['chart_title'], 'out',
        dict([
                (k, None)
                for k in CHART_FIELDS
             ] + [
                (k, dict(aggregate='array'))
                for k in SERIES_FIELDS + ['dataset']
             ] + [
                ('num_datasets', dict(aggregate='count'))
             ]
            )
    ),
    DF.add_computed_field(
        target=dict(
            name='series',
            type='array',
            **{
                'es:itemType': 'object',
                'es:index': False
            }
        ),
        operation=lambda row: [
            dict(
                (k, row[k][i])
                for k in SERIES_FIELDS + ['dataset']
                if len(row[k]) == row['num_datasets']
            )
            for i in range(row['num_datasets'])
        ]
    ),
    DF.delete_fields(SERIES_FIELDS + ['dataset']),
    DF.add_computed_field(
        target=dict(name='doc_id', type='string'),
        operation=lambda row: md5(row['chart_title'].encode('utf8')).hexdigest()[:16]
    ),
    *[
        DF.set_type(f, **{'es:keyword': True})
        for f in ['item_type', 'kind', 'language']
    ],
    DF.set_primary_key(['doc_id']),
    *[
        DF.set_type(f, **{'es:title': True})
        for f in [
            'chart_title', 'chart_title__ar'
        ]
    ],
    DF.validate(),
    DF.update_resource(resources=None,name='datasets'),
)

DATASETS_ES_REVISION = 3

def flow(*_):
    return DF.Flow(
        dataets_flow,
        es_dumper('orgs', DATASETS_ES_REVISION, 'datasets_in_es')
)


