from dataflows import Flow, printer, checkpoint, load, dump_to_path, update_resource, set_type, join
from datapackage_pipelines_migdar.flows.constants import SEARCH_IMPORT_FIELD_NAMES
from tabulator import Stream
from datapackage_pipelines_migdar.flows.common import get_migdar_session


def join_unique_records(*args):
    is_dpp = len(args) > 3
    return Flow(
        load('data/search_import_from_gdrive/datapackage.json', resources=['search_import']),
        load('data/search_results/unique_records.csv', resources=['unique_records']),
        set_type('migdar_id', type='string', resources=['unique_records', 'search_import']),
        join(source_name='search_import', source_key=['migdar_id'],
             target_name='unique_records', target_key=['migdar_id'],
             fields={f'gd_{field}': {'name': field} for field in SEARCH_IMPORT_FIELD_NAMES},
             full=False),
        printer(tablefmt='plain' if is_dpp else 'html', num_rows=1, fields=['migdar_id']),
        dump_to_path('data/unique_records_full'),
        update_resource(None, **{'dpp:streaming': True})
    )


def join_search_app_records(*args):
    is_dpp = len(args) > 3

    def load_search_app_data():
        with Stream('https://migdar-internal-search.odata.org.il/__data/search_app/index.csv',
                    http_session=get_migdar_session()) as index_stream:
            for i, row in enumerate(index_stream.iter()):
                search_id = row[4]
                print(f"#{i}. {search_id} ({row[0]}/{row[1]})")
                url = f'https://migdar-internal-search.odata.org.il/__data/search_app/{search_id}/records.csv'
                with Stream(url, headers=1, http_session=get_migdar_session()) as data_stream:
                    for rownum, row in enumerate(data_stream.iter(keyed=True)):
                        row['migdar_id'] = f'{search_id}-{rownum}'
                        yield row

    return Flow(
        load('data/search_import_from_gdrive/datapackage.json', resources=['search_import']),
        load_search_app_data(),
        update_resource('res_2', name='search_app_records', path='search_app_records.csv'),
        join(source_name='search_import', source_key=['migdar_id'],
             target_name='search_app_records', target_key=['migdar_id'],
             fields={f'gd_{field}': {'name': field} for field in SEARCH_IMPORT_FIELD_NAMES},
             full=False),
        printer(tablefmt='plain' if is_dpp else 'html', num_rows=1, fields=['migdar_id']),
        dump_to_path('data/app_records_full'),
        update_resource(None, **{'dpp:streaming': True})
    )


def flow(parameters, *args):
    return {
        'unique_records': join_unique_records,
        'search_app_records': join_search_app_records
    }[parameters['type']](parameters, *args)
