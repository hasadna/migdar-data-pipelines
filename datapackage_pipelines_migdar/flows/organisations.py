import os
import dataflows as DF
import tabulator
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.i18n import load_tags, split_and_translate

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
    'tags',
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
translations['tags'] = load_tags()


headers = {
 'org_name': ['שם מלא של הארגון - לתרגום או לתעתיק'],
 'org_name__ar': ['اسم الجمعيّة'],
 'entity_id': ['מספר עמותה'],
 'org_kind': ['סוג הארגון'],
 'tagline': ['מטרת הארגון ( משפט תיאורי קצר) - לתרגום'],
 'tagline__ar': ['מטרת הארגון בתרגום לערבית'],
 'objective': ['על הארגון, פעילויות עיקריות ומטרות - לתרגום'],
 'objective__ar': ['על הארגון, פעילויות עיקריות ומטרות - תרגום לערבית'],
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

ORGS_ES_REVISION = 3

org_flow = DF.Flow(
    DF.load(ORGS_URL, name='orgs'), 
    DF.concatenate(headers, resources='orgs', target=dict(name='orgs')),
    *[
        split_and_translate(f, translations[f])
        for f in translations.keys()
        if f != '_'
    ],
    DF.add_computed_field(
        target='doc_id',
        operation='format',
        with_='org/{entity_id}'
    ),
    DF.set_type('org_name',        **{'es:title': True}),
    DF.set_type('org_name__ar',    **{'es:title': True}),
    *[
        DF.set_type(f, **{'es:keyword': True})
        for f in [
            'org_kind', 'life_areas', 'languages', 'tags', 
        ]
    ],
    *[
        DF.set_type(f, **{'es:index': False})
        for f in [
            'org_website', 'org_facebook', 'org_phone_number', 'org_email_address', 'logo_url'

        ]
    ],
    DF.validate(),
)

def flow(*_):
    return DF.Flow(
        org_flow,
        es_dumper('orgs', ORGS_ES_REVISION, 'orgs_in_es')
    )

if __name__ == '__main__':
    DF.Flow(org_flow, DF.printer()).process()