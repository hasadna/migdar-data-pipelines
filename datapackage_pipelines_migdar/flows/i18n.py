import tabulator
import dataflows as DF

TAGS_URL='https://docs.google.com/spreadsheets/d/1tuGksHCNh8GPPtG7YjgInNtT55Oob_KQGJ6r2kNEmNk/view#gid=808755978'

def load_tags():
    tags = list(tabulator.Stream(TAGS_URL).open().iter())
    return [
        [x[1].strip().lower(), x[1], x[2] or x[3], x[0]]
        for x in tags[1:]
        if all(x[:2])
    ] + [
        [x[0].strip().lower(), x[1], x[2] or x[3], x[0]]
        for x in tags[1:]
        if all(x[:2])
    ]

LANGS = ['', '__en', '__ar']
def split_and_translate(field, translations):
    res = DF.Flow(translations, 
                  DF.concatenate({
                    'value': ['col0'], '': ['col1'], '__ar': ['col2'], '__en': ['col3']
                  })
                 ).results()
    translations = res[0][0]
    complained = set()
    
    def process(rows):
        for row in rows:
            vals = row.pop(field) or ''
            vals = vals.split(',')
            for lang in LANGS:
                row['{}{}'.format(field, lang)] = []
            for val in vals:
                val_ = val.strip().lower()
                if not val_:
                    continue
                translation = None
                for t in translations:
                    if t['value'] == val_:
                        translation = t
                        break
                if translation is None:
                    if val not in complained:
                        print('failed to find value for {}: {}'.format(field, val))
                        complained.add(val)
                    for lang in LANGS:
                        row['{}{}'.format(field, lang)].append(val)
                else:
                    for lang in LANGS:
                        if translation[lang] is not None and translation[lang].strip():
                            row['{}{}'.format(field, lang)].append(translation[lang])
                        else:
                            row['{}{}'.format(field, lang)].append(val)
            yield row

    def func(package):
        fields = package.pkg.descriptor['resources'][0]['schema']['fields']
        fields = list(filter(lambda x: x['name'] != field, fields))
        fields.extend([
            {
                'name': '{}{}'.format(field, lang),
                'type': 'array',
                'es:itemType': 'string'
            }
            for lang in LANGS
        ])
        package.pkg.descriptor['resources'][0]['schema']['fields'] = fields
        yield package.pkg
        for res in package:
            yield process(res)
    return func
