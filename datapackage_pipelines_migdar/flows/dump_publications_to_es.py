from datapackage import Package
from dataflows import Flow, load, printer, set_type, update_resource, concatenate, dump_to_path, delete_fields, add_field
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.constants import PUBLICATIONS_ES_REVISION


def split_keyword_list(new_fieldname, fieldname, delimiter=','):
    def splitter():
        def func(row):
            if row.get(fieldname):
                row[new_fieldname] = [
                    x.strip() for x in row[fieldname].split(delimiter)
                ]
            else:
                row[new_fieldname] = []
        return func

    steps = []
    if new_fieldname != fieldname:
        steps.append(add_field(new_fieldname, type='array'))
    steps.append(
        splitter()
    )
    if new_fieldname != fieldname:
        steps.append(delete_fields([fieldname]))
    steps.append(set_type(new_fieldname, type='array', **{'es:itemType': 'string', 'es:keyword': True}))
    return Flow(*steps)

def prefer_gd(field_name):
    def func(row):
        row[field_name] = row.get('gd_{}'.format(field_name)) or row.get(field_name)
    return Flow(
        func, delete_fields(['gd_{}'.format(field_name)])
    )

def main_flow(prefix=''):
    source_url = '{}data/publications_for_es/datapackage.json'.format(prefix)
    package = Package(source_url)
    all_fields = set(
        field.name
        for resource in package.resources
        for field in resource.schema.fields
    )
    all_fields = dict(
        (field_name, [])
        for field_name in all_fields
    )
    return Flow(
        load(source_url),
        lambda row: dict(row, json='{}'),
        concatenate(all_fields, target=dict(name='publications', path='publications.csv')),
        delete_fields(['json']),
        prefer_gd('title'),
        prefer_gd('notes'),
        prefer_gd('publisher'),
        prefer_gd('tags'),
        prefer_gd('language_code'),
        prefer_gd('pubyear'),
        set_type('title',        **{'es:title': True}),
        set_type('notes',        **{'es:hebrew': True}),
        set_type('publisher',    **{'es:keyword': True}),
        split_keyword_list('life_areas', 'gd_Life Domains'),
        split_keyword_list('resource_type', 'gd_Resource Type'),
        split_keyword_list('languages', 'language_code', ' '),
        split_keyword_list('tags', 'tags'),
        printer()
    )

def flow(*args):
    return Flow(
        main_flow(),
        es_dumper('publications', PUBLICATIONS_ES_REVISION, 'published_in_es'),
    )

if __name__=='__main__':
    main_flow('../../').process()