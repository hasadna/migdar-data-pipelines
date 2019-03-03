from dataflows import Flow, printer, checkpoint, set_type, update_resource, dump_to_path, join, load, add_field
import requests
import time


def get_tribunals():
    for tribunal in requests.get('https://judgescv.court.gov.il/assets/static-data/tribunals.json').json():
        yield tribunal


def get_judges():
    for judge in requests.get('https://judgescv.court.gov.il/assets/dynamic-data/judges.json').json():
        yield judge


def fetch_judges_details(rows):
    for i, judge in enumerate(rows):
        time.sleep(0.1)
        judge_details = requests.get(
            'https://judgescv.court.gov.il/assets/dynamic-data/judges/{}.json'.format(judge['Judge_ID'])
        ).json()
        judge_details['Image_As_Base64'] = ''
        yield dict(judge, **judge_details)


def parse_judges_extra_details(row):
    tribunal_type_name = None
    if row.get('Tribunal_Type_Code'):
        if row['Tribunal_Type_Code'] == 1:
            tribunal_type_name = row['Tribunal_Name']
        else:
            tribunal_type_name = {
                2: 'עליון',
                3: 'מחוזי',
                4: 'מיסים',
                5: 'השלום',
                6: 'נוער',
                7: 'משפחה',
                8: 'תעבורה',
                9: 'אזורי לעבודה',
                10: 'ארצי לעבודה',
            }[row['Tribunal_Type_Code']]
    row['tribunal_type_name'] = tribunal_type_name


def judges_flow(out_path):
    return Flow(
        get_tribunals(),
        update_resource(['res_1'], name='tribunals', path='tribunals.csv'),
        checkpoint('judges_tribunals'),
        get_judges(),
        update_resource(['res_2'], name='judges_list', path='judges_list.csv'),
        set_type('Is_In_Dimus_List', resources=['judges_list'], type='boolean'),
        checkpoint('judges_judges_list'),
        join('tribunals', ['Tribunal_Code'], 'judges_list', ['Tribunal_Code'],
             fields={
                 'Tribunal_Type_Code': {},
                 'Tribunal_Arkaa_Code': {'name': 'Arkaa_Code'},
                 'Tribunal_District_Code': {'name': 'District_Code'},
                 'Tribunal_Name': {'name': 'Name'}
             }),
        fetch_judges_details,
        checkpoint('judges_details'),
        add_field('tribunal_type_name', 'string'),
        parse_judges_extra_details,
        checkpoint('judges_extra_details'),
        dump_to_path(out_path),
        printer(num_rows=1)
    )


if __name__ == '__main__':
    judges_flow('data/judges').process()
