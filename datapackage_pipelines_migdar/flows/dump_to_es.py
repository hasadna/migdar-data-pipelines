from dataflows_elasticsearch import dump_to_es
from tableschema_elasticsearch.mappers import MappingGenerator
import dataflows as DF

import logging
import time
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
            if field.get('es:title') or field.get('es:boost'):
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

    @classmethod
    def _update_properties(cls, properties, schema, prefix=''):
        props = super(BoostingMappingGenerator, cls)._update_properties(properties, schema, prefix)
        props['create_timestamp'] = dict(
            index=True,
            type='float'
        )
        return props

class my_dump_to_es(dump_to_es):

    def finalize(self):
        now = time.time()
        for index_name, configs in self.index_to_resource.items():
            for config in configs:
                if 'revision' in config:
                    revision = config['revision']
                    resource_name = config['resource-name']
                    if index_name.endswith('__docs'):
                        continue
                    logging.info('DELETING from "%s" items with revision < %d',
                                index_name, revision)
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
                            }
                        )
                        logging.info('GOT (%d) %r', i, ret)
                logging.info('%s: SETTING CREATE TIMESTAMP in "%s" items', datetime.datetime.now().isoformat(), index_name)
                body = {
                    "script": {
                        "inline": "ctx._source.create_timestamp = params.cur_time",
                        "params": {
                            "cur_time": now
                        }
                    },
                    "query": {
                        "bool": {
                            "must_not": {
                                "exists": {
                                    "field": "create_timestamp"
                                }
                            }
                        }
                    }
                }
                try:
                    ret = self.engine.update_by_query(
                        index_name, body, timeout='5m', request_timeout=300
                    )                    
                except Exception:
                    logging.info('%s: FAILED SETTING CREATE TIMESTAMP in "%s" items', datetime.datetime.now().isoformat(), index_name)
                    raise
                logging.info('UPDATE GOT %r', ret)


    def normalizer(self, resource: ResourceWrapper):
        doc_ids = dict()
        for index_name in self.index_to_resource.keys():
            res = self.engine.search(index=index_name, size=10000)
            for hit in res['hits']['hits']:
                src = hit['_source']
                if 'create_timestamp' not in src:
                    doc_ids[src['doc_id']] = src['create_timestamp']
        logging.info('GOT %d ROWS with TIMESTAMPS', len(doc_ids))
        for row in super().normalizer(resource)():
            if row['doc_id'] in doc_ids:
                row['create_timestamp'] = doc_ids[row['doc_id']]
            yield row

def update_pk(pk):
    def update_schema(package):
        for resource in package.pkg.descriptor['resources']:
            resource['schema']['primaryKey'] = [pk]
        yield package.pkg
        yield from package
    return update_schema


def collate(revision):
    def process(rows):
        for row in rows:
            value = dict(
                (k,v) for k,v in row.items()
                if k not in ('doc_id', 'revision', 'score')
            )
            yield dict(
                doc_id=row['doc_id'],
                revision=revision,
                score=row['score'],
                value=value
            )

    def func(package):
        package.pkg.descriptor['resources'][0]['schema']['fields'] = [
            dict(name='doc_id', type='string'),
            dict(name='revision', type='integer'),
            dict(name='score', type='number'),
            dict(name='value', type='object', **{'es:index': False})
        ]
        yield package.pkg
        for i, res in enumerate(package):
            if i == 0:
                yield process(res)
            else:
                yield res
    return func


def es_dumper(resource_name, revision, path):
    return DF.Flow(
        update_pk('doc_id'),
        DF.add_field('revision', 'integer', default=revision),
        DF.add_field('score', 'number', default=1),
        my_dump_to_es(
            indexes={
                'migdar__' + resource_name: [
                    {
                        'resource-name': resource_name,
                        'revision': revision
                    }
                ]
            },
            mapper_cls=BoostingMappingGenerator,
            index_settings={'index.mapping.coerce': True},
            elasticsearch_options=dict(timeout=60)
        ),
        DF.dump_to_path('data/{}'.format(path)),
        collate(revision),
        my_dump_to_es(
            indexes={
                'migdar__docs': [
                    {
                        'resource-name': resource_name,
                        'revision': revision
                    }
                ]
            },
            mapper_cls=BoostingMappingGenerator,
            index_settings={'index.mapping.coerce': True}
        ),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.printer(),
    )