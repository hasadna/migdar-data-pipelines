import re

from datapackage import Package
from dataflows import Flow, load, printer, set_type, \
    concatenate, delete_fields, add_field, add_computed_field
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.prepare_data_for_es import \
    PUBLICATIONS_ES_REVISION, KEY_PATTERN, PAGE_TITLE_PATTERN
from datapackage_pipelines_migdar.flows.i18n import split_and_translate


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
    steps.append(set_type(new_fieldname, type='array',
                          **{'es:itemType': 'string', 'es:keyword': True}))
    return Flow(*steps)


def prefer_gd(field_name):
    def func(row):
        row[field_name] = (
            row.get('gd_{}'.format(field_name)) or row.get(field_name)
        )
    return Flow(
        func, delete_fields(['gd_{}'.format(field_name)])
    )


years = re.compile('[12][0-9]{3}')


def extract_year(record):
    pubyear = record.get('pubyear')
    if pubyear:
        all_years = years.findall(pubyear)
        if len(all_years):
            return int(all_years[0])


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
        concatenate(all_fields,
                    target=dict(name='publications', path='publications.csv')),
        delete_fields(['json']),
        prefer_gd('title'),
        prefer_gd('notes'),
        prefer_gd('publisher'),
        prefer_gd('tags'),
        prefer_gd('language_code'),
        prefer_gd('pubyear'),
        split_keyword_list('item_kind', 'gd_Item Type'),
        split_keyword_list('life_areas', 'gd_Life Domains'),
        split_keyword_list('source_kind', 'gd_Resource Type'),
        split_keyword_list('languages', 'language_code', ' '),
        split_keyword_list('tags', 'tags'),
        load('data/zotero/zotero.csv'),
        concatenate(
            dict(
                title=[],
                pubyear=[],
                publisher=[],
                author=[],
                life_areas=[],
                notes=[],
                languages=[],
                tags=[],
                url=[],
                migdar_id=[],
                item_kind=[],
                source_kind=[],
                isbn=[],
                physical_description=[],
                publication_distribution_details=[],
                doc_id=[],

            ),
            target=dict(name='publications', path='publications.csv')
        ),
        set_type('title',        **{'es:title': True}),
        set_type('notes',        **{'es:hebrew': True}),
        set_type('publisher',    **{'es:keyword': True}),
        add_field('year', 'integer',
                  default=extract_year),
        split_and_translate('tags', 'tags', keyword=True),
        split_and_translate('life_areas', 'life_areas', keyword=True),
        split_and_translate('languages', 'languages', keyword=True),
        split_and_translate('source_kind', 'source_kind', keyword=True),
        split_and_translate('item_kind', 'item_kind', keyword=True),
        printer(),
        add_computed_field([
            {'operation': 'format', 'target': 'doc_id', 'with': KEY_PATTERN},
            {'operation': 'format', 'target': 'page_title',
             'with': PAGE_TITLE_PATTERN},
        ]),
        add_computed_field([]),
    )


def flow(*args):
    return Flow(
        main_flow(),
        es_dumper('publications', PUBLICATIONS_ES_REVISION, 'published_in_es'),
    )


if __name__ == '__main__':
    main_flow('../../').process()
