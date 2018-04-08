from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import logging
import pymarc
import json


parameters, datapackage, resources, stats = ingest() + ({},)


topics = {row['topic']: row['match_type'] for row in next(resources) if row['topic'] is not None}
logging.info("{} topics".format(len(topics)))


topic_stats = {}


def get_resource():
    for resource in resources:
        for row in resource:
            for topic in topics:
                if row['tags'] is not None and topic.strip() in row['tags'].lower():
                    row['topic'] = topic
                    yield row
                    topic_stats.setdefault(topic, {}).setdefault('num_items', 0)
                    topic_stats[topic]['num_items'] += 1
                    break


def get_stats_resource():
    for topic, _stats in topic_stats.items():
        yield {'topic': topic, 'num_items': _stats['num_items']}


datapackage['resources'][0] = datapackage['resources'][1]
del datapackage['resources'][1]
datapackage['resources'][0]['schema']['fields'] += [{'name': 'topic', 'type': 'string'}]
datapackage['resources'] += [{PROP_STREAMING: True, 'name': 'topic_stats', 'path': 'topic_stats.csv',
                              'schema': {'fields': [{'name': 'topic', 'type': 'string'},
                                                    {'name': 'num_items', 'type': 'integer'}]}}]


spew(datapackage, (get_resource(),get_stats_resource(),), stats)
