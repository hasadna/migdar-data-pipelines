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
    tree.write('data/sitemap.{}.xml'.format(rows.res.name),
                encoding='utf-8', xml_declaration=True)


def lang_flow(lang, prefix):

    tags = [dict(doc_id=list(k)) for k in sorted(set(
            (prefix, x['hebrew'], x[lang])
            for x in translations['tags'].values()
        ))]

    def add_url(prefix_):
        def func(rows):
            for row in rows:
                if 'url' not in row:
                    yield row
                elif row.get('doc_id'):
                    row['url'] = 'https://yodaat.org/{}item/{}'.format(prefix_, row['doc_id'])
                    yield row
                else:
                    print('MMMMMMMM MISSING DOC ID', row)

        return DF.Flow(
            DF.add_field('url', 'string', resources=-1),
            func,
        )

    return DF.Flow(
        *[
            DF.Flow(
                DF.load('https://api.yodaat.org/data/{}_in_es/data/{}.csv'.format(x, y), name='{}-{}'.format(x, lang)),
                add_url(prefix)
            )
            for x, y in [
                ('publications', 'publications'),
                ('orgs', 'orgs'),
                ('datasets', 'out')
            ]
        ],
        tags,
        DF.add_field('url', 'string',
                     lambda row: 'https://yodaat.org/{}search?tag={}&itag={}&kind=all&filters={{}}&sortOrder=-year'.format(*row.get('doc_id')),
                     resources=-1),
        DF.update_resource(-1, name='tags-{}'.format(lang)),
    )


def flow(*_):
    with open('data/sitemap.xml', 'w') as index:
        index.write("""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n""")
        for kind in ('publications', 'orgs', 'datasets', 'tags'):
            for lang in ('hebrew', 'english', 'arabic'):
                index.write("""<sitemap><loc>https://api.yodaat.org/data/sitemap.{}-{}.xml</loc></sitemap>\n""".format(kind, lang))
        index.write("""</sitemapindex>""")
    return DF.Flow(
        lang_flow('hebrew', ''),
        lang_flow('english', 'en/'),
        lang_flow('arabic', 'ar/'),

        registerSiteMaps,
        DF.select_fields(['url']),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.printer()
    )


if __name__ == '__main__':
    flow().results()
