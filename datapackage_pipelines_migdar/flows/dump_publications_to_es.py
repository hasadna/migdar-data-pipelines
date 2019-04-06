from datapackage import Package
from dataflows import Flow, load, printer, set_type, update_resource, concatenate, dump_to_path, delete_fields
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.constants import PUBLICATIONS_ES_REVISION


def split_keyword_list(fieldname, delimiter=','):
    def func(package):
        new_name = fieldname + '_list'
        package.pkg.descriptor['resources'][0]['schema']['fields'].append({
            'name': new_name,
            'type': 'array',
            'es:itemType': 'string',
            'es:keyword': True
        })
        yield package.pkg
        for resource in package:
            yield map(lambda row: dict([*row.items(),
                                        (new_name, list(map(lambda s: s.strip(),
                                                            row.get(fieldname).split(delimiter) 
                                                            if row.get(fieldname)
                                                            else [])))
                                       ]),
                      resource)
    return func

def flow(*args):
    is_dpp = len(args) > 3
    source_url = 'data/publications_for_es/datapackage.json'
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
        set_type('title',        **{'es:title': True}),
        set_type('gd_title',     **{'es:title': True}),
        set_type('notes',        **{'es:hebrew': True}),
        set_type('gd_notes',     **{'es:hebrew': True}),
        set_type('publisher',    **{'es:keyword': True}),
        set_type('gd_publisher', **{'es:keyword': True}),
        split_keyword_list('gd_Life Domains'),
        split_keyword_list('gd_Resource Type'),
        split_keyword_list('gd_language_code', ' '),
        split_keyword_list('language_code', ' '),
        split_keyword_list('gd_tags'),
        split_keyword_list('tags'),
        es_dumper('publications', PUBLICATIONS_ES_REVISION, 'published_in_es'),
        printer(tablefmt='plain' if is_dpp else 'html', num_rows=1, fields=['doc_id'])
    )
