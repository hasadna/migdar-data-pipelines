import os
import subprocess

import dataflows as DF

SCREENSHOT = os.path.join(os.path.dirname(__file__), 'node', 'screenshot.js')

def do_screenshot():
    def func(rows):
        for row in rows:
            for lang in ['', 'ar/', 'en/']:
                doc_id = row['doc_id']
                url = f'https://yodaat.org/{lang}card/{doc_id}'
                outpath = os.path.join('data', lang + os.path.dirname(doc_id))
                os.makedirs(outpath, exist_ok=True)
                outpath = os.path.join('data', lang + doc_id + '.png')
                subprocess.call(['node', SCREENSHOT, url, outpath, '.card'])
        return []
    return func


def flow(*_, path='data/datasets_in_es'):
    return DF.Flow(
        DF.load('{}/datapackage.json'.format(path)),
        do_screenshot(),
        DF.update_resource(-1, **{'dpp:streaming': True})
    )


if __name__ == '__main__':
    flow(path='https://api.yodaat.org/data/datasets_in_es').process()