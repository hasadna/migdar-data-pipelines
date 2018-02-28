from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pathlib import Path
import logging
import pymarc
import json
import os


parameters, datapackage, resources, stats = ingest() + ({},)


stats.update(num_written_records=0,
             num_pages=0)


os.makedirs(parameters["out-path"], exist_ok=True)
cur_page_records = 0
cur_page_num = 1
records_resource = next(resources)
while True:
    incr_page = False
    with open(parameters["out-path"] + "/kvinnsam{}.dat".format(cur_page_num), 'wb') as f:
        marc_writer = pymarc.MARCWriter(f)
        for row in records_resource:
            for record in pymarc.JSONReader(json.dumps([row["record"]])):
                marc_writer.write(record)
                stats["num_written_records"] += 1
                cur_page_records += 1
            if cur_page_records >= parameters["records-per-page"]:
                incr_page = True
                break
        marc_writer.close()
    if incr_page:
        cur_page_num += 1
        cur_page_records = 0
        stats["num_pages"] += 1
    else:
        break


spew(dict(datapackage, resources=[]), [], stats)
