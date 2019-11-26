import os
import re
from copy import copy

import googleapiclient.discovery
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from openpyxl import load_workbook
import logging
from dataflows import (
    Flow, printer, filter_rows, add_field, load,
    concatenate, set_type, add_computed_field
)
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.i18n import split_and_translate, fix_urls
from datapackage_pipelines_migdar.flows.constants import REVISION

KEY_PATTERN = 'publications/{migdar_id}'
PAGE_TITLE_PATTERN = '{title}'

SCOPES = ['https://www.googleapis.com/auth/drive']
try:
    credentials = service_account.Credentials.from_service_account_file('/migdar-gdrive/secret-g-service-account.json', scopes=SCOPES)
except Exception as e:
    logging.exception('Failed to open creds!')
    credentials = service_account.Credentials.from_service_account_file('gdrive_creds.json', scopes=SCOPES)
drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)


def list_gdrive():
    results = drive_service.files().list(q="'16bSopg9nlQDBN8gsjW712xuBWy16gPW0' in parents", fields='files(id,kind,name,mimeType,modifiedTime)').execute()
    yield from results.get('files', [])


def download_files():
    os.makedirs('pubfiles', exist_ok=True)

    def func(row):
        filename = row['filename']
        if not os.path.exists(filename):
            print('Downloading', filename)
            with open(filename, 'wb') as f:
                request = drive_service.files().get_media(fileId=row['id'])
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()

    return func


def one(i):
    return len(list(filter(lambda x: x, i))) == 1


def get_sheets():
    def func(rows):
        for row in rows:
            print('Attempting with %r' % row)
            wb = load_workbook(row['filename'])
            for sheet_name in wb.sheetnames:
                if 'deleted' in sheet_name.strip().lower():
                    continue
                row = copy(row)
                row['sheet'] = sheet_name
                row['headers'] = None
                sheet = wb[sheet_name]
                for i, cells in enumerate(sheet.rows, start=1):
                    headers = [x.value for x in cells]
                    if not any(headers):
                        continue
                    assert one(x in headers for x in ['Domain', 'Life Domains']), 'DOMAIN %r' % list(zip(headers, [x.value for x in list(sheet.rows)[i+1]]))
                    if 'migdar_id' not in headers:
                        print('BAD HEADERS', row['name'], sheet_name)
                        continue
                    if i > 3:
                        break
                    row['headers'] = i
                    break
                if row.get('headers') is not None:
                    yield row
                    break
    return func


years = re.compile('[12][0-9]{3}')


def extract_year(record):
    pubyear = record.get('pubyear')
    if isinstance(pubyear, int):
        return str(pubyear)
    else:
        all_years = years.findall(str(pubyear))
        if len(all_years):
            return all_years[0]
        else:
            print('YEAR?? %r' % pubyear)


def fix_nones():
    def func(row):
        return dict(
            (k, None if v == 'None' else v)
            for k, v in row.items()
        )
    return func


def base_flow():
    sources, *_ = Flow(
        list_gdrive(),
        filter_rows(lambda row: row['kind'] == 'drive#file' and row['mimeType'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
        add_field('filename', 'string',
                  default=lambda row: 'pubfiles/{modifiedTime}-{id}.xlsx'.format(**row)),
        download_files(),
        add_field('sheet', 'string'),
        add_field('headers', 'integer', 1),
        get_sheets(),
    ).results()
    return Flow(
        *[
            load(source['filename'],
                 sheet=source['sheet'],
                 headers=source['headers'],
                 infer_strategy=load.INFER_STRINGS,
                 cast_strategy=load.CAST_TO_STRINGS,
                 name=source['filename'])
            for source in sources[0]
        ],
        filter_rows(lambda row: row.get('migdar_id') not in ('', 'None', None)),
        load('data/zotero/zotero.csv'),
        concatenate(
            fields={
                'migdar_id': [],
                'title': ['Title', ],
                'bib_title': [],
                'bib_related_parts': [],

                'notes': [],
                'tags': ['Tags'],
                'publisher': [],
                'languages': ['language_code'],
                'item_kind': ['Item Type', 'Item type', 'item_type'],
                'pubyear': ['pubyear/pubdate'],
                'life_areas': ['Life Domains', 'Domain'],
                'source_kind': ['Resource Type', 'Resource type'],
                'authors': ['author'],
                'url': ['URL'],

                # 'publication_distribution_details',
                # 'custom_metadata',
                # 'physical_description',
                # 'bib_place_publisher_date',
                # 'bib_standard_technical_report_number',

            },
            target=dict(
                name='publications',
                path='data/publications.csv'
            )
        ),
        fix_nones(),
        fix_urls(['url']),
        # split_keyword_list('item_kind'),
        # split_keyword_list('life_areas'),
        # split_keyword_list('source_kind'),
        # split_keyword_list('languages', ' '),
        # split_keyword_list('tags'),
        set_type('title',        **{'es:title': True}),
        set_type('authors',       **{'es:boost': True}),
        set_type('notes',        **{'es:hebrew': True}),
        set_type('publisher',    **{'es:boost': True}),
        add_field('year', 'integer',
                  default=extract_year),
        split_and_translate('tags', 'tags', keyword=True, delimiter=','),
        split_and_translate('life_areas', 'life_areas', keyword=True, delimiter=','),
        split_and_translate('languages', 'languages', keyword=True, delimiter=' '),
        split_and_translate('source_kind', 'source_kind', keyword=True),
        split_and_translate('item_kind', 'item_kind', keyword=True),
        add_computed_field([
            {'operation': 'format', 'target': 'doc_id', 'with': KEY_PATTERN},
            {'operation': 'format', 'target': 'page_title',
             'with': PAGE_TITLE_PATTERN},
        ]),
        add_field('title_kw', 'string',
                  default=lambda row: row.get('title'),
                  **{'es:keyword': True}),
    )


def flow(*_):
    return Flow(
        base_flow(),
        es_dumper('publications', REVISION, 'publications_in_es')
    )


if __name__ == '__main__':
    Flow(
        base_flow(),
        printer(),
    ).process()
