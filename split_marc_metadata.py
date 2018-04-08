from datapackage_pipelines.wrapper import ingest, spew
import logging
import pymarc
import json
import os


parameters, datapackage, resources, stats = ingest() + ({},)


os.makedirs(parameters["out-path"], exist_ok=True)
last_topic, dat_file = None, None
for row in next(resources):
    if row['topic'] != last_topic:
        if dat_file:
            dat_file.close()
        dat_file = open(parameters["out-path"] + "/nli_{}.dat".format(row['topic']), 'wb')
        marc_writer = pymarc.MARCWriter(dat_file)
        last_topic = row['topic']
    assert marc_writer
    json_records_string = '[' + json.loads(row["json"]) + ']'
    for record in pymarc.JSONReader(json_records_string):
        marc_writer.write(record)


spew(dict(datapackage, resources=[]), [], stats)
