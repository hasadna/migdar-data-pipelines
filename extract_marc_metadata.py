from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import logging
import pymarc
import json


parameters, datapackage, resources, stats = ingest() + ({},)

# https://www.loc.gov/marc/bibliographic/
# Relevant fields for NLI z3950 data
# fields with less then 3 chars will cause to extract all the fields that match this prefix
# all the strings will be appended together
nli_metadata_fields = {
    '041': 'language_code',
    '09' : 'custom_metadata',  # seems to contain details like type of item, availability online
    '260': 'publication_distribution_details',
    '300': 'physical_description',  # details about the file format
    '5'  : 'notes',
    '650': 'tags',
    '856': 'url',
    '992': 'url',
}


common_metadata_fields = [{'name': 'language_code', 'type': 'string'},
                          {'name': 'custom_metadata', 'type': 'string'},
                          {'name': 'publication_distribution_details', 'type': 'string'},
                          {'name': 'physical_description', 'type': 'string'},
                          {'name': 'notes', 'type': 'string'},
                          {'name': 'tags', 'type': 'string'},
                          {'name': 'url', 'type': 'string'},]


def get_resource():
    for resource in resources:
        for row in resource:
            raw = {f['name']: [] for f in common_metadata_fields}
            json_records_string = '['+json.loads(row["json"])+']'
            for record in pymarc.JSONReader(json_records_string):
                for field in record.get_fields():
                    for match_tag in [field.tag, field.tag[:2], field.tag[:1]]:
                        field_name = nli_metadata_fields.get(match_tag)
                        if field_name:
                            raw_values = raw.setdefault(field_name, [])
                            raw_values += [field.format_field()]
            row.update(**{k: ', '.join(v) for k, v in raw.items()})
            yield row


datapackage['resources'][0].update(name='marc_metadata', path='marc_metadata.csv')
datapackage['resources'][0]['schema']['fields'] += [{'name': 'language_code', 'type': 'string'},
                                                    {'name': 'custom_metadata', 'type': 'string'},
                                                    {'name': 'publication_distribution_details', 'type': 'string'},
                                                    {'name': 'physical_description', 'type': 'string'},
                                                    {'name': 'notes', 'type': 'string'},
                                                    {'name': 'tags', 'type': 'string'},
                                                    {'name': 'url', 'type': 'string'},]


spew(datapackage, (get_resource(),), stats)
