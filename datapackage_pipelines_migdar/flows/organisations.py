import os
import dataflows as DF
import tabulator
from datapackage_pipelines_migdar.flows.dump_to_es import DumpToElasticSearch

os.environ.setdefault('DPP_ELASTICSEARCH', 'localhost:19200')

ORGS_URL='https://docs.google.com/spreadsheets/d/1fWHl6rlvpqfCXoM1IVhqlY0SWQ_IYCWukuyCcTDwWjM/view'
LEGEND_URL='https://docs.google.com/spreadsheets/d/1fWHl6rlvpqfCXoM1IVhqlY0SWQ_IYCWukuyCcTDwWjM/edit#gid=1243311724'

legend = list(tabulator.Stream(LEGEND_URL).open().iter())

translations_order = [
    'org_kind',
    'regions',
    'life_areas',
    'languages',
    'specialties',
    'provided_services',
    'target_audiences',
    '_'
]
translations = {}
current = None
for line in legend:
    if any(x is not None and x.strip() for x in line):
        if current is None:
            current = translations_order.pop(0)
            translations[current] = []
        else:
            translations[current].append(line)
    else:
        current = None

LANGS = ['', '.en', '.ar']
def split_and_translate(field, translations):
    res = DF.Flow(translations, 
                  DF.concatenate({
                    'value': ['col0'], '': ['col1'], '.ar': ['col2'], '.en': ['col3']
                  })
                 ).results()
    translations = res[0][0]
    complained = set()
    
    def process(rows):
        for row in rows:
            vals = row.pop(field)
            vals = vals.split(',')
            for lang in LANGS:
                row['{}{}'.format(field, lang)] = []
            for val in vals:
                val = val.strip()
                translation = None
                for t in translations:
                    if t['value'].strip() == val:
                        translation = t
                        break
                if translation is None:
                    if val not in complained:
                        print('failed to find value for {}: {}'.format(field, val))
                        complained.add(val)
                    for lang in LANGS:
                        row['{}{}'.format(field, lang)].append(val)
                else:
                    for lang in LANGS:
                        if translation[lang] is not None and translation[lang].strip():
                            row['{}{}'.format(field, lang)].append(translation[lang])
                        else:
                            row['{}{}'.format(field, lang)].append(val)
            yield row
    
    def func(package):
        fields = package.pkg.descriptor['resources'][0]['schema']['fields']
        fields = list(filter(lambda x: x['name'] != field, fields))
        fields.extend([
            dict(
                name='{}{}'.format(field, lang),
                type='array'
            )
            for lang in LANGS
        ])
        package.pkg.descriptor['resources'][0]['schema']['fields'] = fields
        yield package.pkg
        for res in package:
            yield process(res)
    return func

headers = {
 'name': ['שם מלא של הארגון - לתרגום או לתעתיק'],
 'name.ar': ['اسم الجمعيّة'],
 'entity_id': ['מספר עמותה'],
 'org_kind': ['סוג הארגון'],
 'tagline': ['מטרת הארגון ( משפט תיאורי קצר) - לתרגום'],
 'tagline.ar': ['מטרת הארגון בתרגום לערבית'],
 'objective': ['על הארגון, פעילויות עיקריות ומטרות - לתרגום'],
 'objective.ar': ['על הארגון, פעילויות עיקריות ומטרות - תרגום לערבית'],
 'life_areas': ['תחומי חיים'],
 'languages': ['שפות בהן ניתנים שירותים'],
 'specialties': ['תחומי פעילות והתמחות עיקריים'],
 'target_audiences': ['קהלי יעד'],
 'provided_services': ['השירותים הניתנים - אתר יודעת'],
 'regions': ['אזור גיאוגרפי'],
 'year_founded': ['שנת הקמה'],
 'tags': ['תגיות - התרגום הוא בקובץ נפרד'],
 'hotline_phone_number': ['מספר הטלפון של הקו החם: - رقم هاتف الخط الدافئ:'],
 'org_website': ['לינק לאתר הארגון'],
 'org_facebook': ['לינק לדף פייסבוק של הארגון'],
 'org_phone_number': ['טלפון ליצירת קשר עם הארגון'],
 'org_email_address': ['מייל ליצירת קשר עם הארגון'],
 'logo_url': ['לוגו'],
}

ORGS_ES_REVISION = 1


def update_pk(pk):
    def update_schema(package):
        for resource in package.pkg.descriptor['resources']:
            resource['schema']['primaryKey'] = [pk]
        yield package.pkg
        yield from package
    return update_schema



def flow(*_):
    return DF.Flow(
        DF.load(ORGS_URL, name='orgs'), 
        DF.concatenate(headers, resources='orgs', target=dict(name='orgs')),
        *[
            split_and_translate(f, translations[f])
            for f in translations.keys()
            if f != '_'
        ],
        DF.add_computed_field([
            dict(
                target='doc_id',
                operation='format',
                with_='org/{entity_id}'
            )
        ]),
        set_type('name',        **{'es:title': True}),
        set_type('name.ar',     **{'es:title': True}),
        set_type('name.en',     **{'es:title': True}),
        *[
            set_type('gd_notes',    **{'es:itemType': 'string'}),
            for f in translations.keys()
            if f != '_'
        ],
        update_pk('doc_id'),
        DumpToElasticSearch({'migdar': [{'resource-name': 'orgs',
                                         'doc-type': 'orgs',
                                         'revision': ORGS_ES_REVISION}]})(),
        DumpToElasticSearch({'migdar': [{'resource-name': 'orgs',
                                         'doc-type': 'document',
                                         'revision': ORGS_ES_REVISION}]})(),
        dump_to_path('data/orgs_in_es'),
        update_resource(None, **{'dpp:streaming': True})
)
