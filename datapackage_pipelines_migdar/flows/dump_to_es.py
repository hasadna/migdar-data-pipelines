from datapackage_pipelines_elasticsearch.processors.dump.to_index import ESDumper
from tableschema_elasticsearch.mappers import MappingGenerator
import dataflows as DF

import logging
import collections
import datetime


class BoostingMappingGenerator(MappingGenerator):

    def __init__(self):
        super(BoostingMappingGenerator, self).__init__(base={})

    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        prop = super(BoostingMappingGenerator, cls)._convert_type(schema_type, field, prefix)
        if field.get('es:keyword'):
            prop['type'] = 'keyword'
            if field.get('es:title'):
                prop['boost'] = 100
        elif schema_type == 'string':
            if field.get('es:title'):
                prop['boost'] = 100
            if field.get('es:title') or field.get('es:hebrew'):
                prop['fields'] = {
                    "hebrew": {
                    "type": "text",
                    'analyzer': 'hebrew'
                }
            }
        elif schema_type in ('number', 'integer'):
            prop['index'] = True
        return prop



class DumpToElasticSearch(ESDumper):
    
    def __init__(self, indexes, **parameters):
        parameters['indexes'] = indexes
        self.mapper_cls = BoostingMappingGenerator
        self.index_settings = {'index.mapping.coerce': True}
        self.__params = parameters
        self.stats = {}
        counters = self.__params.get('counters', {})
        self.datapackage_rowcount = counters.get('datapackage-rowcount', 'count_of_rows')
        self.datapackage_bytes = counters.get('datapackage-bytes', 'bytes')
        self.datapackage_hash = counters.get('datapackage-hash', 'hash')
        self.resource_rowcount = counters.get('resource-rowcount', 'count_of_rows')
        self.resource_bytes = counters.get('resource-bytes', 'bytes')
        self.resource_hash = counters.get('resource-hash', 'hash')
        self.add_filehash_to_path = self.__params.get('add-filehash-to-path', False)

    def __call__(self):

        def step(package):
            self.initialize(self.__params)
            self.__datapackage = self.prepare_datapackage(package.pkg.descriptor, self.__params)
            yield package.pkg
            for resource in package:
                resource.spec = resource.res.descriptor
                ret = self.handle_resource(self.schema_validator(resource),
                                           resource.res.descriptor, self.__params, package.pkg.descriptor)
                ret = self.row_counter(package.pkg.descriptor, resource.res.descriptor, ret)
                yield ret
            self.finalize()

        return step

    
    def format_datetime_rows(self, spec, rows):
        yield from rows
        # formatters = {}
        # for f in spec['schema']['fields']:
        #     if f['type'] == 'datetime':
        #         logging.info('FIELD datetime: %r', f)
        #         if f.get('format', 'default') in ('any', 'default'):
        #             formatters[f['name']] = lambda x: None if x is None else x.strftime('%Y-%m-%dT%H:%M:%SZ')
        #         else:
        #             def formatter(f):
        #                 fmt = f['format']
        #                 def func(x):
        #                     if x is None:
        #                         return None
        #                     else:
        #                         return x.strftime(fmt)
        #                 return func
        #             formatters[f['name']] = formatter(f)
        # id = lambda x: x
        #
        # for row in rows:
        #     yield dict((k, formatters.get(k, id)(v)) for k, v in row.items())

    def initialize(self, parameters):
        parameters['reindex'] = False
        return super(DumpToElasticSearch, self).initialize(parameters)

    def handle_resource(self, resource, spec, parameters, datapackage):
        return super(DumpToElasticSearch, self)\
                .handle_resource(resource, 
                                 spec, parameters, datapackage)

    def finalize(self):
        for index_name, configs in self.index_to_resource.items():
            for config in configs:
                if 'revision' in config:
                    revision = config['revision']
                    doc_type = config['doc-type']
                    logging.info('DELETING from "%s", "%s" items with revision < %d',
                                 index_name, doc_type, revision)
                    queries = [
                        {
                            "bool": {
                                "must_not": {
                                    "exists": {
                                        "field": "revision"
                                    }
                                }
                            }
                        },
                        {
                            "range": {
                                "revision": {
                                    "lt": revision
                                }
                            }
                        }
                    ]
                    for i, q in enumerate(queries):
                        ret = self.engine.delete_by_query(
                            index_name,
                            {
                                "query": q
                            },
                            doc_type=doc_type
                        )
                        logging.info('GOT (%d) %r', i, ret)


def update_pk(pk):
    def update_schema(package):
        for resource in package.pkg.descriptor['resources']:
            resource['schema']['primaryKey'] = [pk]
        yield package.pkg
        yield from package
    return update_schema


def collate():
    def process(rows):
        for row in rows:
            value = dict(
                (k,v) for k,v in row.items()
                if k not in ('doc_id', 'revision')
            )
            yield dict(
                doc_id=row['doc_id'],
                score=1,
                value=value
            )
    def func(package):
        package.pkg.descriptor['resources'][0]['schema']['fields'] = [
            dict(name='doc_id', type='string'),
            dict(name='score', type='number'),
            dict(name='value', type='object', **{'es:index': False})
        ]
        yield package.pkg
        for res in package:
            yield process(res)
    return func


def es_dumper(resource_name, revision, path):
    return DF.Flow(
        update_pk('doc_id'),
        DF.add_field('revision', 'integer', default=revision),
        DumpToElasticSearch({'migdar': [{'resource-name': resource_name,
                                         'doc-type': resource_name,
                                         'revision': revision}]})(),
        DF.dump_to_path('data/{}'.format(path)),
        collate(),
        DumpToElasticSearch({'migdar': [{'resource-name': resource_name,
                                         'doc-type': 'document',
                                         'revision': revision}]})(),
        DF.update_resource(None, **{'dpp:streaming': True})
    )