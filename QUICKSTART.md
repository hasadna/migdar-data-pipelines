
## Initialize credentials to access migdar data

It's recommended to start your notebook like this to  set the env vars:

```
env MIGDAR_USERNAME="" MIGDAR_PASSWORD="" jupyter lab
```


```python
MIGDAR_PASSWORD = %env MIGDAR_PASSWORD
MIGDAR_USERNAME = %env MIGDAR_USERNAME

assert len(MIGDAR_USERNAME) > 5 and len(MIGDAR_PASSWORD) > 5
```

#### Create an quthorized requests session


```python
import requests

migdar_session = requests.Session()
migdar_session.auth = (MIGDAR_USERNAME, MIGDAR_PASSWORD)
```

## Download the aggregated search results before moving to the internal search app


```bash
%%bash -s "$MIGDAR_USERNAME" "$MIGDAR_PASSWORD"
MIGDAR_USERNAME="${1}"
MIGDAR_PASSWORD="${2}"

# set to "-0" to download all rows (It's 470MB)
LIMIT_ROWS="3000"

mkdir -p data/search_results
wget -qO /dev/stdout \
     --http-user=${MIGDAR_USERNAME} --http-password=${MIGDAR_PASSWORD} \
     https://migdar-internal-search.odata.org.il/__data/search_results/unique_records.csv \
| head -n${LIMIT_ROWS} \
> data/search_results/unique_records.csv
```


```python
from dataflows import Flow, load, printer, dump_to_path
from collections import deque

def head(rows):
    for rownum, row in enumerate(rows):
        if rownum >= 2:
            break
        yield row

Flow(load('data/search_results/unique_records.csv'), head,
     printer(tablefmt='html', fields=['title', 'pubyear', 'migdar_id'])).process()[1]
```


<h3>unique_records</h3>



<table>
<thead>
<tr><th style="text-align: right;">  #</th><th>title
(string)                                                                                          </th><th style="text-align: right;">     pubyear
(string)</th><th style="text-align: right;">  migdar_id
(integer)</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">  1</td><td>הוראות בלתי שגרתיות לתפירת צואר הרחם                                                     </td><td style="text-align: right;">1981</td><td style="text-align: right;">0</td></tr>
<tr><td style="text-align: right;">  2</td><td>שינויים בזרימת הדם בשורר הרחם בעקבות הזרקת פרוסטגלנדין ALPHA F2 להפסקת הריון בטרימסטר שני</td><td style="text-align: right;">1984</td><td style="text-align: right;">1</td></tr>
</tbody>
</table>





    {}



## Load the data from GDrive

These are the column names we store from the user-entered xlsx files on GDrive


```python
SEARCH_IMPORT_FIELD_NAMES = ['Life Domains', 'Resource Type', 'Item Type', 'title', 'pubyear', 'publisher', 'author', 
                             'language_code', 'custom_metadata', 'publication_distribution_details', 'notes', 'tags', 
                             'url', 'migdar_id', 'item_type', 'first_ccl_query', 'marc_856']
```


```python
from dataflows import Flow, printer, load, dump_to_path, update_resource
from tabulator import Stream
from openpyxl import load_workbook
import tempfile
import logging
import requests
import os

def process_row(row):
    return {k: str(row[k]) if row.get(k) else '' for k in SEARCH_IMPORT_FIELD_NAMES}


def sheet_iterator(first_row, header_row, stream_iter, filename, sheet_name, stream):
    def inner_sheet_iterator():
        if first_row:
            yield process_row(dict(zip(header_row, first_row)))
        for row in stream_iter:
            yield process_row(dict(zip(header_row, row)))
    for i, row in enumerate(inner_sheet_iterator()):
        if not row['migdar_id']:
            if row['title']:
                print('    {}/{}#{}: missing migdar_id'.format(filename, sheet_name, i))
        else:
            yield row
    stream.close()


def load_from_gdrive_files(rows):
    if rows.res.name == 'search_import_index':
        for row_index, row in enumerate(rows, start=1):
#             if row_index !=5:
#                 continue
            file_url = f"https://migdar-internal-search.odata.org.il/__data/search_import/{row['name']}"
            print(file_url)
            with tempfile.NamedTemporaryFile('w+b', suffix='.xlsx') as temp_file:
                with migdar_session.get(file_url, stream=True) as response:
                    for chunk in response.iter_content():
                        temp_file.write(chunk)
                temp_file.flush()
                wb = load_workbook(temp_file.name)
                for sheet_number, sheet_name in enumerate(wb.sheetnames, start=1):
                    if 'deleted' in sheet_name.strip().lower():
                        continue
                    stream = Stream(temp_file.name, sheet=sheet_name)
                    stream.open()
                    print('#{}.{}/{}: loading sheet'.format(row_index, row['name'], sheet_name))
                    stream_iter = stream.iter()
                    try:
                        first_row = next(stream_iter)
                    except StopIteration:
                        first_row = None
                    if first_row:
                        if 'migdar_id' not in first_row and sheet_number > 1:
                            header_row = first_sheet_header_row
                        else:
                            header_row = first_row
                            first_row = None
                            if sheet_number == 1:
                                first_sheet_header_row = header_row
                        # print(header_row)
                        # for k in header_row:
                        #     if k and k not in [f['name'] for f in schema['fields']]:
                                # print('found field: {}'.format(k))
                        #         field_type = 'string'
                        #         schema['fields'].append({'name': k, 'type': field_type})
                        yield from sheet_iterator(first_row, header_row, stream_iter, row['name'], sheet_name, stream)
    else:
        yield from rows
                            

Flow(
    load('https://migdar-internal-search.odata.org.il/__data/search_import/index.csv', http_session=migdar_session),
    update_resource('index', name='search_import_index', path='search_import_index.csv'),
    load_from_gdrive_files,
    update_resource('search_import_index', name='search_import', path='search_import.csv',
                schema={'fields': [{'name': n,  'type': 'string'} for n in SEARCH_IMPORT_FIELD_NAMES]}),
    printer(num_rows=1, tablefmt='html', fields=['migdar_id', 'pubyear', 'title']),
    dump_to_path('data/search_import_from_gdrive')
).process()[1]
```


<h3>search_import</h3>


    https://migdar-internal-search.odata.org.il/__data/search_import/2018-10-16- ORLY - Feminism and Science_HEB.xlsx
    #1.2018-10-16- ORLY - Feminism and Science_HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-30- ORLY - Sex discriminationHEB.xlsx
    #2.2018-08-30- ORLY - Sex discriminationHEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-29- Talia - Employee rights_heb.xlsx
    #3.2018-08-29- Talia - Employee rights_heb.xlsx/Sheet1: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-10- Havatzelet- Feminsm  HEB.xlsx
    #4.2018-08-10- Havatzelet- Feminsm  HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-26 women_committee_background_material.xlsx
    #5.2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate: loading sheet
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#0: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#7: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#9: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#44: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#69: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#73: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#75: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#87: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#92: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#105: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#107: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#109: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#116: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#129: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#132: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#135: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#140: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#142: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#150: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#153: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#158: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#162: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#166: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#168: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#171: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#176: missing migdar_id
        2018-08-26 women_committee_background_material.xlsx/women_committee_background_mate#178: missing migdar_id
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-26  women_committee_protocols.xlsx
    #6.2018-08-26  women_committee_protocols.xlsx/women_committee_protocols: loading sheet
        2018-08-26  women_committee_protocols.xlsx/women_committee_protocols#0: missing migdar_id
        2018-08-26  women_committee_protocols.xlsx/women_committee_protocols#1: missing migdar_id
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-16- Havatzelet- Feminist Criticism  HEB.xlsx
    #7.2018-08-16- Havatzelet- Feminist Criticism  HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-09-03- Havatzelet- Femininity  HEB.xlsx
    #8.2018-09-03- Havatzelet- Femininity  HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-12- Talia - Domestic relations courts heb  .xlsx
    #9.2018-08-12- Talia - Domestic relations courts heb  .xlsx/רשימת פריטים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-29- ORLY -buisenesswomen - ENG.xlsx
    #10.2018-05-29- ORLY -buisenesswomen - ENG.xlsx/מקוטלגים: loading sheet
    #10.2018-05-29- ORLY -buisenesswomen - ENG.xlsx/Sheet2: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-07-30- Ayelet - Family violence  heb.xlsx
    #11.2018-07-30- Ayelet - Family violence  heb.xlsx/Sheet1: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-23- Havatzelet- Feminist Theory  HEB.xlsx
    #12.2018-08-23- Havatzelet- Feminist Theory  HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-08-02- Havatzelet- ECOFEMINISM.xlsx
    #13.2018-08-02- Havatzelet- ECOFEMINISM.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-07-22- Ayelet - Diversity in the workplace  heb.xlsx
    #14.2018-07-22- Ayelet - Diversity in the workplace  heb.xlsx/Sheet1: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-18- Havatzelet-Christian Women.xlsx
    #15.2018-06-18- Havatzelet-Christian Women.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-04- Havatzelet- Arab-Israeli conflict_WOMEN_ HEB.xlsx
    #16.2018-06-04- Havatzelet- Arab-Israeli conflict_WOMEN_ HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-07- Havatzelet -buisenesswomen HEB.xlsx
    #17.2018-06-07- Havatzelet -buisenesswomen HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-07-24- Talia -Discrimination in employment heb.xlsx
    #18.2018-07-24- Talia -Discrimination in employment heb.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-25- Ayelet -Empowerment HEB.xlsx
    #19.2018-06-25- Ayelet -Empowerment HEB.xlsx/Sheet1: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-11- Havatzelet- Cleaning Contract.xlsx
    #20.2018-06-11- Havatzelet- Cleaning Contract.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-07- Ayelet - Decision making WOMEN heb.xlsx
    #21.2018-06-07- Ayelet - Decision making WOMEN heb.xlsx/Sheet1: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-03- Talia - Abused Women ENG - modified.xlsx
    #22.2018-05-03- Talia - Abused Women ENG - modified.xlsx/מקוטלגים: loading sheet
    #22.2018-05-03- Talia - Abused Women ENG - modified.xlsx/Deteted Items: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-24- Havatzelet- Age_and-employment.xlsx
    #23.2018-05-24- Havatzelet- Age_and-employment.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-07- Ayelet - CHRONIC PAIN.xlsx
    #24.2018-06-07- Ayelet - CHRONIC PAIN.xlsx/Sheet1: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-04 - Ayelet - civil military.xlsx
    #25.2018-06-04 - Ayelet - civil military.xlsx/Sheet1: loading sheet
        2018-06-04 - Ayelet - civil military.xlsx/Sheet1#0: missing migdar_id
        2018-06-04 - Ayelet - civil military.xlsx/Sheet1#2: missing migdar_id
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-01-Havatzelet-Abortion.xlsx
    #26.2018-05-01-Havatzelet-Abortion.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-06-04- Havatzelet- Arab-Israeli conflict_WOMEN_ eng.xlsx
    #27.2018-06-04- Havatzelet- Arab-Israeli conflict_WOMEN_ eng.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-07_Work and Family-Ayelet2.xlsx
    #28.2018-05-07_Work and Family-Ayelet2.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-09- ORLY - Adoption HEB.xlsx
    #29.2018-05-09- ORLY - Adoption HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-28 Havatzelet-Age_Discrimination-employment.xlsx
    #30.2018-05-28 Havatzelet-Age_Discrimination-employment.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-22- Havatzelet- Affirmative_HEB.xlsx
    #31.2018-05-22- Havatzelet- Affirmative_HEB.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-22- Havatzelet- Affirmative_eng.xlsx
    #32.2018-05-22- Havatzelet- Affirmative_eng.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-17- Havatzelet- Abused Women HEB2.xlsx
    #33.2018-05-17- Havatzelet- Abused Women HEB2.xlsx/מקוטלגים: loading sheet
    https://migdar-internal-search.odata.org.il/__data/search_import/2018-05-16- Havatzelet- Abused Women HEB1.xlsx
    #34.2018-05-16- Havatzelet- Abused Women HEB1.xlsx/מקוטלגים: loading sheet



<table>
<thead>
<tr><th>#   </th><th>title
(string)                                                                     </th><th>pubyear
(string)                      </th><th style="text-align: right;">      migdar_id
(string)</th></tr>
</thead>
<tbody>
<tr><td>1   </td><td>לעומתיות : רישומים מתרבות הנגד של השמאל הפמיניסטי בישראל /          </td><td>2009                 </td><td style="text-align: right;">32208</td></tr>
<tr><td>2   </td><td>"אתה השלום עכשיו שלי" : מעמד נשים והשיחים הפמיניסטיים בארגוני שמאל /</td><td>Academic Institutions</td><td style="text-align: right;">32216</td></tr>
<tr><td>... </td><td>                                                                    </td><td>                     </td><td style="text-align: right;">     </td></tr>
<tr><td>2043</td><td>חצויה /                                                             </td><td>2015 תשע"ה.          </td><td style="text-align: right;"> 2016</td></tr>
</tbody>
</table>





    {'count_of_rows': 2043,
     'bytes': 2588867,
     'hash': 'feffafe0168180ba84ef5fdc26309c3a',
     'dataset_name': None}



## Join with the source data

The GDrive data contains the user-entered data, this joins it with the source data and full marc data


```python
from dataflows import Flow, load, join, printer, set_type

Flow(
    load('data/search_import_from_gdrive/datapackage.json', resources=['search_import']),
    load('data/search_results/unique_records.csv', resources=['unique_records']),
    set_type('migdar_id', type='string', resources=['unique_records', 'search_import']),
    join(source_name='search_import', source_key=['migdar_id'],
         target_name='unique_records', target_key=['migdar_id'],
         fields={f'gd_{field}': {'name': field} for field in SEARCH_IMPORT_FIELD_NAMES},
         full=False),
    printer(tablefmt='html', num_rows=1, fields=['title', 'migdar_id', 'gd_Life Domains']),
    dump_to_path('data/unique_records_full')
).process()[1]
```


<h3>unique_records</h3>



<table>
<thead>
<tr><th>#  </th><th>title
(string)                                                            </th><th style="text-align: right;">     migdar_id
(string)</th><th>gd_Life Domains
(string)                </th></tr>
</thead>
<tbody>
<tr><td>1  </td><td>הפסקות הריון על פי החוק : 2003-1990 /                      </td><td style="text-align: right;"> 178</td><td>Health         </td></tr>
<tr><td>2  </td><td>הפסקות הריון על פי החוק : 2004-1990 /                      </td><td style="text-align: right;"> 186</td><td>Health         </td></tr>
<tr><td>...</td><td>                                                           </td><td style="text-align: right;">    </td><td>               </td></tr>
<tr><td>389</td><td>The enigma woman the death sentence of Nellie May Madison /</td><td style="text-align: right;">2993</td><td>Gender Violence</td></tr>
</tbody>
</table>





    {'count_of_rows': 389,
     'bytes': 2754961,
     'hash': '1ea8cfc5ffc6e950e3691e827c4f63e2',
     'dataset_name': None}



## Join with the search app data

**there isn't any data yet from the search app (Oct 23, 2018)**


```python
from tabulator import Stream
from dataflows import Flow, printer, checkpoint, load, dump_to_path, update_resource, set_type, join

!{'rm -rf .checkpoints/gdrive_search_app_source_data'}

def load_search_app_data():
    with Stream('https://migdar-internal-search.odata.org.il/__data/search_app/index.csv', 
                http_session=migdar_session) as index_stream:
        for i, row in enumerate(index_stream.iter()):
#             if i > 5:
#                 continue
            search_id = row[4]
            print(f"#{i}. {search_id} ({row[0]}/{row[1]})")
            url = f'https://migdar-internal-search.odata.org.il/__data/search_app/{search_id}/records.csv'
            with Stream(url, headers=1, http_session=migdar_session) as data_stream:
                for rownum, row in enumerate(data_stream.iter(keyed=True)):
                    row['migdar_id'] = f'{search_id}-{rownum}'
                    yield row

Flow(
    load('data/search_import_from_gdrive/datapackage.json', resources=['search_import']),
    load_search_app_data(),
    update_resource('res_2', name='search_app_records', path='search_app_records.csv'),
    checkpoint('gdrive_search_app_source_data'),
    join(source_name='search_import', source_key=['migdar_id'],
         target_name='search_app_records', target_key=['migdar_id'],
         fields={f'gd_{field}': {'name': field} for field in SEARCH_IMPORT_FIELD_NAMES},
         full=False),
    printer(tablefmt='html', num_rows=1, fields=['migdar_id']),
    dump_to_path('data/app_records_full')
).process()[1]
```

    saving checkpoint to: .checkpoints/gdrive_search_app_source_data
    #0. 0fb41d0f8f2d4fdbb444b5605ace15f2 (asdfisjdofijsadfasdf/heb)
    #1. becab2849d1a403fba5e3a73d6dfa77e (asdfisjdofijsadfasdf/heb)
    #2. 83596544daf448caafdead9409f953a8 (discrimination against black women/heb)



<h3>search_app_records</h3>


    #3. d05c183d5ba548dea3e649fdd738ca54 (discrimination/heb)
    #4. 42ce23d9dc924e1ab2ce856990399180 (safasdfasdfsadf/heb)
    #5. 56cf98417e0e4884ba7d44c4882f687a (adoption black women/heb)
    #6. b07130541dd74e1ba33ad1130279721b (adoption women/heb)
    #7. 75b8f2db4f9a45b78fc522dbabcbf80f (abortion/arb)
    #8. 5a63c01f0717490abf593130675cba6d (discrimination against women/heb)
    #9. da40ead706194a9baf20a9e77c1f5549 ("נשים"/heb)
    #10. 0791e557229146448975e81206350af9 ("אפליה נגד נשים"/heb)
    #11. 72b0fca7a3ce47cc99b46046054f1891 (discrimination/heb)
    #12. 2f0b05980d164dce96daf49459e2ee0e ("אפליה נגד נשים פלשתינאיות"/heb)
    #13. c254f0d6249c48478db58d31869bb892 ("מחשבים"/heb)
    #14. cec11054239e4822933db80b86d80312 ("מחשבים"/heb)
    #15. d5225b0fbf0c483492864d0763085a89 ("הדרה של נשים"/heb)
    #16. eb78226b2a5946fea12fd723facebdb5 ("הדרה של נשים"/heb)
    #17. 14ab8e3e925347d8acf003579fb685aa ("בדואיות"/arb)
    #18. 58591008756d40769448495d607d81bc ("בדואיות"/arb)
    #19. c626a9bc80314103b05eda44f3f039bc ("אפליה נשים"/heb)
    #20. d4248c45b3e745748ba49820e3a79440 (-/ara)
    #21. 483406b84dfc4a5fb40a1136d43cce70 (-/ara)
    #22. 461d5ae6d809408d868cc48c02bf6229 ( israel, women/ara)
    #23. cf2628c6294d47869866439ca384cf68 (test/heb)
    #24. a08da65030a04b2892366193efd1812d (test one two three/heb)
    #25. 07d798a364bf4339a4ea9dbdb72866d1 (Testing one two three/heb)
    #26. 6b3fb163c5bb4b8392578cca18c1ffb2 (testing one two three/heb)
    #27. 3e7c28beb1624b4abe8a6cb0e0eedd17 ( Abortion -- Law and legislation -- Fiction/heb)
    #28. 51b8eed647974641b8cd77ca7d946168 ( Abortion -- Law and legislation -- Fiction/ara)
    #29. a249beb23ec84c219be68da747f3b6e0 ( Abortion -- Law and legislation -- Fiction/eng)
    #30. a71b243c63fd4e3684ab70a3c44ca276 ( Abortion/heb)
    #31. df07df4b810f40d1a1574c7bb95aa418 ( Abortion/ara)
    #32. 2acbc9a1532240c88800822e330599be ( feminism/ara)
    #33. 306c6d3d879f432981778322ab9b5020 ( abortion/heb)
    #34. 33896be11f73435cb75784c01e8379cf ( abortion/heb)
    #35. 680a0c6982ef480bb1f8f8f9b29f05c0 ( Abortion/ara)
    #36. ddeefb036b584e449562519057bf83f2 ( Abortion/ara)
    #37. 464ecabc8a23442684c2dcc9b55a7a8f ( Women/ara)
    #38. 30fb2dc28d104fffbbe53351f5d7acca ( Women/eng)
    #39. c91d0dee4b09418f87f381e65ad19e6a ( Women, Abortion/eng)
    #40. bcedd0599dbb49eab6d140b2a618a967 ( Abuse/heb)
    #41. e6056d6a081140a1b40b4f7f89a52288 ( abuse/heb)
    #42. dd2e68f40c544f7e95d15fa905c2b187 ( abuse/ara)
    #43. 58ec02f10121457e9c00c49c83bdc41b ( abuse/ara)
    #44. 40195ef2e2b74b868132a8cf3c3a014a (בדיקה אחת שתיים שלוש/heb)
    #45. dae4717e4ca64b658d8839d64ae3c44b ( abuse, women/heb)
    #46. df58525f3a2f4dcfbf70c1f1b67a7276 ( Feminist/ara)
    #47. 659bbcc44b93441289c991f689fdd740 ( Budget/heb)
    #48. ca66e7cdfa06458b97ac52f2a10c3705 ( Budget -- Sex differences/heb)
    #49. 703ccc54ced34e86a4e19ec372d5ced4 ( Budget  , Sex differences/heb)
    #50. cce36cedeead4272a7f5f03c7abd6c04 ( Budget  , Sex differences/ara)
    #51. ae0abf1932904ea6b3619b1523da7fd5 ( Feminist/ara)
    #52. 4c54819e3fc9452abcf8b4e9bced5390 ( Municipal government , differences/heb)
    #53. 6c5f3f929f1542d492cad16b64c2908b ( Body image/heb)
    #54. 69228363a55d401d90d1144ce52c50b7 ( Body image, Locus of control/heb)



<table>
<thead>
<tr></tr>
</thead>
<tbody>
</tbody>
</table>


    checkpoint saved: gdrive_search_app_source_data





    {'count_of_rows': 0,
     'bytes': 4996,
     'hash': '215e660d2db2c2e316aecdbeb771453d',
     'dataset_name': None}


