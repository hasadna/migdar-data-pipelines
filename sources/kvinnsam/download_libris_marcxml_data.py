from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import logging, requests, shutil, os
import xml.etree.ElementTree as ET


LIBRIS_API_URL_TEMPLATE = "http://api.libris.kb.se/xsearch?format=marcxml" \
                          "&n={results_per_page}" \
                          "&start={start}" \
                          "&format_level={format_level}" \
                          "&query={query}"


def get_libris_url(query, start=1):
    return LIBRIS_API_URL_TEMPLATE.format(results_per_page=200,  # The number of records to be retrieved, to a maximum of 200.
                                          start=start, # Specifies the starting position in a hitlist from which records should be taken.
                                          format_level="full",  # brief | full
                                                                #  Specifies whether “see” or ”see also” links to
                                                                # authority forms (9XX fields) should be included.
                                          holdings="true",  # Specifies whether holding information should be included.
                                          query=query)  # Search query as defined by the query language.
                                                        # http://librishelp.libris.kb.se/help/search_language_eng.jsp


def parse_libris_page(*args, **kwargs):
    data = requests.get(get_libris_url(*args, **kwargs)).text
    root = ET.fromstring(data)
    total_records = int(root.attrib["records"])
    return data, root, total_records


def save_libris_page(file_name, *args, **kwargs):
    data, root, total_records = parse_libris_page(*args, **kwargs)
    with open(file_name, "w") as f:
        f.write(data)
    return total_records


parameters, datapackage, resources, stats = ingest() + ({},)


total_records, start = None, 1

while not total_records or start < total_records:
    if start == 1:
        file_name = parameters["out-path"] + "/pages/start1.xml"
    else:
        file_name = parameters["out-path"] + "/pages/start{}/{}/{}.xml".format(str(start)[0], str(start)[1], start)
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    total_records = save_libris_page(file_name, parameters["query"], start)
    start += 200
    logging.info("start={} total_records={}".format(start, total_records))


spew(datapackage, resources, stats)
