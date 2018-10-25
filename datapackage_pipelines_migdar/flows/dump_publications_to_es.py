from dataflows import Flow, load, printer, set_type, update_resource
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
    return Flow(
        load('data/publications_for_es/datapackage.json'),
        lambda row: dict(row, json=''),
        update_schema,
        set_type('json', resources=None, type='object', **{'es:index': False}),
        DumpToElasticSearch({'migdar': [{'resource-name': 'unique_records',
                                         'doc-type': 'publications',
                                         'revision': PUBLICATIONS_ES_REVISION}]})(),
        DumpToElasticSearch({'migdar': [{'resource-name': 'search_app_records',
                                         'doc-type': 'publications',
                                         'revision': PUBLICATIONS_ES_REVISION}]})(),
        printer(tablefmt='plain' if is_dpp else 'html', num_rows=1, fields=['doc_id']),
        update_resource(None, **{'dpp:streaming': True})
    )
