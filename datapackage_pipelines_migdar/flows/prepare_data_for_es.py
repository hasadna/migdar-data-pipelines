from dataflows import (Flow, load, printer, add_field, dump_to_sql,
                       filter_rows, add_computed_field, dump_to_path,
                       update_resource)
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError
from kvfile import KVFile
import logging
import hashlib
import datetime
from unidecode import unidecode
import re
from datapackage_pipelines_migdar.flows import constants
from datapackage_pipelines_migdar.flows.common import DATAFLOWS_DB_ENGINE


DB_TABLE = constants.PUBLICATIONS_DB_TABLE
PUBLICATIONS_ES_REVISION = 18
KEY_FIELDS = ['migdar_id']
HASH_FIELDS = None
FILTER_NEXT_UPDATE_DAYS = None
KEY_PATTERN = constants.PUBLICATIONS_KEY_PATTERN
PAGE_TITLE_PATTERN = constants.PUBLICATIONS_PAGE_TITLE_PATTERN


now = datetime.datetime.now()


STATUS_FIELDS = [
    {'name': '__last_updated_at', 'type': 'datetime'},
    {'name': '__last_modified_at', 'type': 'datetime'},
    {'name': '__created_at', 'type': 'datetime'},
    {'name': '__is_new', 'type': 'boolean'},
    {'name': '__is_stale', 'type': 'boolean'},
    {'name': '__staleness', 'type': 'integer'},
    {'name': '__next_update_days', 'type': 'integer'},
    {'name': '__hash', 'type': 'string'},
]
STATUS_FIELD_NAMES = [f['name'] for f in STATUS_FIELDS]


def get_all_existing_ids(connection_string, db_table, key_fields, db_status_fields):
    db_fields = key_fields + db_status_fields
    stmt = ' '.join(['select', ','.join(db_fields), 'from', db_table])
    engine = create_engine(connection_string)
    ret = KVFile()
    try:
        rows = engine.execute(stmt)
        for row in rows:
            rec = dict(zip(db_fields, row))
            existing_id = dict(
                (k, v) for k, v in rec.items()
                if k in db_status_fields
            )
            key = calc_key(rec, key_fields)
            ret.set(key, existing_id)
    except ProgrammingError:
        print('WARNING: Failed to fetch existing keys')
    except OperationalError:
        print('WARNING: Failed to fetch existing keys')
    return ret


class LineSelector():

    def __init__(self):
        self.x = 1

    def __call__(self, i):
        i += 1
        if i - self.x > 10:
            self.x *= 10
        return 0 <= i - self.x <= 10


def calc_key(row, key_fields):
    key = '|'.join(str(row[k]) for k in key_fields)
    return key


def calc_hash(row, hash_fields):
    hash_fields = sorted(hash_fields)
    hash_src = '|'.join(str(row[k]) for k in hash_fields)
    if len(hash_src) > 0:
        hash = hashlib.md5(hash_src.encode('utf8')).hexdigest()
    else:
        hash = ''
    return hash


def process_resource(res, key_fields, hash_fields, existing_ids, prefix):
    count_existing = 0
    count_modified = 0
    count_new = 0
    count_total = 0
    count_stale = 0

    ls = LineSelector()

    for i, row in enumerate(res):
        key = calc_key(row, key_fields)
        hash = calc_hash(row, hash_fields)
        count_total += 1

        debug = ls(i)

        if debug:
            logging.info('#%d: KEY: %r HASH: %r', i, key, hash)
        try:
            existing_id = existing_ids.get(key)
            row.update(existing_id)
            days_since_last_update = (now - existing_id[prefix + '__last_updated_at']).days
            days_since_last_update = max(0, days_since_last_update)
            next_update_days = existing_id[prefix + '__next_update_days']
            next_update_days = min(next_update_days, 90)
            is_stale = days_since_last_update > next_update_days
            overdue = max(1, days_since_last_update - next_update_days)
            staleness = int(100000 + 100000 / (1 + overdue))
            if debug:
                logging.info('#%d: PROPS: %r', i, existing_id)
                logging.info('#%d: >> is_stale: %r, staleness: %r, next_update_days: %r',
                             i, is_stale, staleness, next_update_days)
            if is_stale:
                count_stale += 1
            row.update({
                prefix + '__is_new': False,
                prefix + '__is_stale': is_stale,
                prefix + '__staleness': staleness,
                prefix + '__last_updated_at': now,
                prefix + '__hash': hash,
            })
            if hash == existing_id[prefix + '__hash']:
                row.update({
                    prefix + '__next_update_days': days_since_last_update + 2
                })
                count_existing += 1
                # if not is_stale and days_since_last_update < 7:
                #     continue
            else:
                row.update({
                    prefix + '__last_modified_at': now,
                    prefix + '__next_update_days': 1
                })
                count_modified += 1

        except KeyError:
            row.update({
                prefix + '__is_new': True,
                prefix + '__is_stale': True,
                prefix + '__staleness': 100000,
                prefix + '__last_updated_at': now,
                prefix + '__last_modified_at': now,
                prefix + '__created_at': now,
                prefix + '__next_update_days': 1,
                prefix + '__hash': hash,
            })
            count_new += 1
        if row[prefix + '__is_stale']:
            if debug:
                logging.info('#%d>> %r', i, dict(
                    (k, v) for k, v in row.items()
                    if k.startswith(prefix + '__')
                ))
        yield row

    logging.info('MANAGE REVISION STATS:')
    logging.info('| TOTAL  : %7d', count_total)
    logging.info('| NEW    : %7d', count_new)
    logging.info('| CHANGED: %7d', count_modified)
    logging.info('| SAME   : %7d', count_existing)
    logging.info('| STALE  : %7d', count_stale)


def manage_revisions(package):
    schema_fields = package.pkg.get_resource('unique_records').descriptor['schema']['fields']
    input_key_fields = KEY_FIELDS
    if HASH_FIELDS is None:
        input_hash_fields = set(f['name'] for f in schema_fields)
    input_hash_fields = set(input_hash_fields) - set(input_key_fields)
    input_hash_fields = set(input_hash_fields) - set(STATUS_FIELD_NAMES)
    db_key_fields = input_key_fields
    db_hash_fields = input_hash_fields
    db_status_fields = ['__last_updated_at', '__next_update_days', '__hash', '__created_at']
    existing_ids = get_all_existing_ids(DATAFLOWS_DB_ENGINE, DB_TABLE, db_key_fields, db_status_fields)
    yield package.pkg
    for rows in package:
        yield process_resource(rows, input_key_fields, input_hash_fields, existing_ids, '')


def set_revisions(row):
    for k in ['__last_updated_at', '__last_modified_at', '__created_at']:
        if k in row and row[k]:
            row['rev' + k[1:]] = row[k].date()


def get_pubyear(record):
    pubyear = record.get('pubyear')
    if pubyear:
        pubyear = unidecode(pubyear)
        res = re.match('.*([12][0-9][0-9][0-9]).*', pubyear)
        if res:
            return int(res.group(1))
        else:
            logging.warning('invalid year: {}'.format(pubyear))
            return None
    else:
        return None


def add_date_range():
    return Flow(
        add_field('__date_range_from', 'date'),
        add_field('__date_range_to', 'date'),
        add_field('__date_range_months', 'array', **{'es:itemType': 'string', 'es:keyword': True}),
        lambda row: dict(row,
                         __date_range_from='{}-01-01'.format(get_pubyear(row)),
                         __date_range_to='{}-12-31'.format(get_pubyear(row)),
                         __date_range_months=["{}-{:0>2}".format(get_pubyear(row), i) for i in range(1, 13)])
    )


def flow(*args):
    is_dpp = len(args) > 3
    return Flow(
        load('data/unique_records_full/datapackage.json', resources=['unique_records']),
        load('data/app_records_full/datapackage.json', resources=['search_app_records']),
        add_field('__revision', 'integer', PUBLICATIONS_ES_REVISION),
        *(add_field(f['name'], f['type']) for f in STATUS_FIELDS),
        manage_revisions,
        *(dump_to_sql({DB_TABLE: {'resource-name': resource_name,
                                  'mode': 'update',
                                  'update_keys': KEY_FIELDS}},
                      DATAFLOWS_DB_ENGINE) for resource_name in ['unique_records', 'search_app_records']),
        *(add_field(f'rev_{name}', 'date')
          for name in ['last_updated_at', 'last_modified_at', 'created_at']),
        set_revisions,
        filter_rows(equals=[{'__next_update_days': FILTER_NEXT_UPDATE_DAYS}]) if FILTER_NEXT_UPDATE_DAYS else None,
        add_computed_field([{'operation': 'format', 'target': 'doc_id', 'with': KEY_PATTERN}]),
        add_computed_field([{'operation': 'format', 'target': 'page_title', 'with': PAGE_TITLE_PATTERN}]),
        add_date_range(),
        dump_to_path('data/publications_for_es'),
        printer(tablefmt='plain' if is_dpp else 'html', num_rows=1, fields=['doc_id']),
        update_resource(None, **{'dpp:streaming': True})
    )
