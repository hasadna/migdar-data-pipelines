import dataflows as DF
import xml.etree.cElementTree as ET
import datetime


def registerSiteMaps(rows):
    root = ET.Element('urlset')
    root.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
    root.attrib['xsi:schemaLocation'] = 'http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd'
    root.attrib['xmlns'] = 'http://www.sitemaps.org/schemas/sitemap/0.9'

    for row in rows:
        doc_id = row['doc_id']
        url = 'https://yodaat.org/item/' + doc_id
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
        registerSiteMaps,
        DF.printer()
    )


if __name__ == '__main__':
    flow().results()
