from datapackage import Package
from dataflows import Flow, load, printer, set_type, update_resource, concatenate, dump_to_path, delete_fields
from datapackage_pipelines_migdar.flows.dump_to_es import DumpToElasticSearch
from datapackage_pipelines_migdar.flows.constants import PUBLICATIONS_ES_REVISION
import os


os.environ.setdefault('DPP_ELASTICSEARCH', 'localhost:19200')


def update_schema(package):
    for resource in package.pkg.descriptor['resources']:
        resource['schema']['primaryKey'] = ['migdar_id']
    yield package.pkg
    yield from package


def flow(*args):
    is_dpp = len(args) > 3
    source_url = 'data/publications_for_es/datapackage.json'
    package = Package(source_url)
    all_fields = set(
        field.name
        for resource in package.resources
        for fields in resource.schema.fields
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
        update_schema,
        DumpToElasticSearch({'migdar': [{'resource-name': 'publications',
                                         'doc-type': 'publications',
                                         'revision': PUBLICATIONS_ES_REVISION}]})(),
        printer(tablefmt='plain' if is_dpp else 'html', num_rows=1, fields=['doc_id']),
        dump_to_path('data/published_in_es'),
        update_resource(None, **{'dpp:streaming': True})
    )
