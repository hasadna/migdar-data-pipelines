import os
import dataflows as DF
import tabulator
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.i18n import \
    split_and_translate, clean

ORGS_URL='https://docs.google.com/spreadsheets/d/1fWHl6rlvpqfCXoM1IVhqlY0SWQ_IYCWukuyCcTDwWjM/view'

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
 'alt_name1': ['שם נוסף1'],
 'alt_name2': ['שם נוסף2'],
 'alt_name3': ['שם נוסף3'],
 'alt_name4': ['שם נוסף4'],
 'alt_name5': ['שם נוסף5'],
}

ORGS_ES_REVISION = 3

org_flow = DF.Flow(
    DF.load(ORGS_URL, name='orgs'), 
    DF.concatenate(headers, resources='orgs', target=dict(name='orgs')),
    DF.add_field(
        'alt_names', 'array',
        default=lambda r: [
            r[x]
            for x in [
                'alt_name%d' % i
                for i in range(1, 6)
            ] + ['org_name']
            if x in r and r[x]
        ]
    ),
    DF.add_field('compact_services', 'string', lambda row: row.get('provided_services'))
    DF.delete_fields(['alt_name[1-5]']),
    *[
        split_and_translate(
            f, f, 
            delimiter=',',
            keyword=f in ('org_kind', 'life_areas', 'languages', 'tags', 'compact_services')
        )
        for f in ('languages', 'life_areas', 'tags', 'regions', 'org_kind',
                  'specialties', 'provided_services', 'target_audiences', 'compact_services')
    ],
    DF.add_computed_field(
        target='doc_id',
        operation='format',
        with_='org/{entity_id}'
    ),
    DF.set_type('org_name',        **{'es:title': True}),
    DF.set_type('org_name__ar',    **{'es:title': True}),
    DF.set_type('alt_names',       
                **{'es:itemType': 'string', 'es:title': True}),
    *[
        DF.set_type(f, **{'es:index': False})
        for f in [
            'org_website', 'org_facebook', 'org_phone_number',
            'org_email_address', 'logo_url'

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
