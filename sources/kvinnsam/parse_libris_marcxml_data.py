from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pathlib import Path
import logging
import pymarc


parameters, datapackage, _, stats = ingest() + ({},)


marcxml_file_names = list(Path(parameters["path"] + "/pages").glob('**/*.xml'))


def get_record_row(int_pubyear, record):
    yield {"pubyear": int_pubyear,
           "title": record.title(),
           "record": record.as_dict()}


def get_records_resource():
    stats.update(missing_pubyear=0,
                 int_pubyear_over_2012=0,
                 int_pubyear_under_2013=0,
                 str_pubyear_over_2012=0,
                 str_pubyear_invalid=0)
    for file_name in marcxml_file_names:
        for record in pymarc.parse_xml_to_array(str(file_name)):
            pubyear = record.pubyear()
            if pubyear:
                try:
                    int_pubyear = int(pubyear)
                except:
                    int_pubyear = None
                if int_pubyear:
                    if int_pubyear > 2012:
                        stats["int_pubyear_over_2012"] += 1
                        yield from get_record_row(int_pubyear, record)
                    else:
                        stats["int_pubyear_under_2013"] += 1
                else:
                    int_pubyear = None
                    for year in range(2013, 2018):
                        if str(year) in pubyear:
                            int_pubyear = year
                            break
                    if int_pubyear:
                        stats["str_pubyear_over_2012"] += 1
                        yield from get_record_row(int_pubyear, record)
                    else:
                        stats["str_pubyear_invalid"] += 1
            else:
                stats["missing_pubyear"] += 1



resources = [get_records_resource()]


datapackage["resources"] = [{PROP_STREAMING: True,
                             "name": "records",
                             "path": "records.csv",
                             "schema": {"fields": [{"name": "pubyear", "type": "integer"},
                                                   {"name": "title", "type": "string"},
                                                   {"name": "record", "type": "object"},]}}]


spew(datapackage, resources, stats)
