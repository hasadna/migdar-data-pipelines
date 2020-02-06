import dataflows as DF
import xml.etree.cElementTree as ET
import datetime

from datapackage_pipelines_migdar.flows.i18n import translations


def registerSiteMaps(rows):
    root = ET.Element('urlset')
    root.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
    root.attrib['xsi:schemaLocation'] = 'http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd'
    root.attrib['xmlns'] = 'http://www.sitemaps.org/schemas/sitemap/0.9'

    for row in rows:
        url = row['url']
        dt = datetime.datetime.now().strftime('%Y-%m-%d')
        doc = ET.SubElement(root, 'url')
        ET.SubElement(doc, 'loc').text = url
        ET.SubElement(doc, 'lastmod').text = dt
        ET.SubElement(doc, 'changefreq').text = 'weekly'
        ET.SubElement(doc, 'priority').text = '1.0'
        yield row

    tree = ET.ElementTree(root)
    tree.write('data/sitemap.xml', encoding='utf-8', xml_declaration=True)


def flow(*_):
    return DF.Flow(
        *[
            DF.load('https://api.yodaat.org/data/{}_in_es/data/{}.csv'.format(x, y))
            for x, y in [
                ('publications', 'publications'),
                ('orgs', 'orgs'),
                ('datasets', 'out')
            ]
        ],
        DF.concatenate(dict(doc_id=[])),
        DF.add_field('url', 'string', lambda row: 'https://yodaat.org/item/' + row['doc_id'], resources=-1),
        (dict(doc_id=k) for k in sorted({v['hebrew'] for v in translations['tags'].values()})),
        DF.add_field('url', 'string', lambda row: 'https://yodaat.org/search?tag=%s&kind=all&filters={}&sortOrder=-year' % row['doc_id'], resources=-1),
        DF.concatenate(dict(url=[])),
        registerSiteMaps,

        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.printer()
    )


if __name__ == '__main__':
    flow().results()
