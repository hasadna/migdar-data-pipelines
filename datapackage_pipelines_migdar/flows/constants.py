import os


SEARCH_IMPORT_FIELD_NAMES = ['Life Domains', 'Resource Type', 'Item Type', 'title', 'pubyear', 'publisher', 'author',
                             'language_code', 'custom_metadata', 'publication_distribution_details', 'notes', 'tags',
                             'url', 'migdar_id', 'item_type', 'first_ccl_query', 'marc_856']


with open(os.path.join(os.path.dirname(__file__), 'PUBLICATIONS_REVISION.txt')) as f:
    PUBLICATIONS_ES_REVISION = int(f.read().strip())
PUBLICATIONS_DB_TABLE = '_elasticsearch_mirror__publications'
PUBLICATIONS_KEY_PATTERN = 'publications/{migdar_id}'
PUBLICATIONS_PAGE_TITLE_PATTERN = '{title}'
