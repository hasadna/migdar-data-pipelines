import os
import re
from copy import copy

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaIoBaseDownload

from openpyxl import load_workbook
import logging
from dataflows import (
    Flow, printer, filter_rows, add_field, load,
    concatenate, set_type, add_computed_field, parallelize
)
from datapackage_pipelines_migdar.flows.dump_to_es import es_dumper
from datapackage_pipelines_migdar.flows.i18n import split_and_translate, fix_urls
from datapackage_pipelines_migdar.flows.common import fix_links
from datapackage_pipelines_migdar.flows.constants import REVISION

KEY_PATTERN = 'publications/{migdar_id}'
PAGE_TITLE_PATTERN = '{title}'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
try:
    credentials = Credentials.from_service_account_file(
        '/migdar-gdrive/secret-g-service-account.json', scopes=SCOPES)
except Exception:
    logging.exception('Failed to open creds!')
    credentials = Credentials.from_service_account_file(
        'gdrive_creds.json', scopes=SCOPES)

service = build('sheets', 'v4', credentials=credentials)

GOOGLE_SHEETS_ID = '1IPRvpogUZ06R9zVRPdZeYfAwdrs9hx0iRB8zSFubl_o'

def list_all_sheet_ids(google_doc_id):
    # Get all 'gid' numbers of the google spreadsheet:
    spreadsheet = service.spreadsheets().get(
        spreadsheetId=google_doc_id,
        fields="sheets(properties(sheetId,title))"
    ).execute()
    ret = []
    for sheet in spreadsheet['sheets']:
        props = sheet['properties']
        print(f"{props['title']} → gid={props['sheetId']}")
        # append ret the full url of the sheet
        ret.append(f"https://docs.google.com/spreadsheets/d/{google_doc_id}/edit#gid={props['sheetId']}")
    return ret


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


def verify_migdar_id():
    def func(row):
        if len(row['migdar_id']) > 200:
            print('TOO LONG MIGDAR ID', row)
            row['migdar_id'] = row['migdar_id'][:200]
    return func


def base_flow():
    sources = list_all_sheet_ids(GOOGLE_SHEETS_ID)
    return Flow(
        *[
            load(source,
                 infer_strategy=load.INFER_STRINGS,
                 cast_strategy=load.CAST_TO_STRINGS,
                 name=source.split('#')[1].split('=')[1])
            for source in sources
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
                'languages': [],
                'item_kind': [],
                'pubyear': [],
                'life_areas': [],
                'source_kind': [],
                'authors': [],
                'url': [],

            },
            target=dict(
                name='publications',
                path='data/publications.csv'
            )
        ),
        fix_nones(),
        fix_urls(['url']),
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
        fix_links('notes'), 
        verify_migdar_id(),
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

