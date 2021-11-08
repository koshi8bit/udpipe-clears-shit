import requests
from googleapiclient.errors import HttpError
from pandas import Int64Index

from google_sheets import GoogleSheets
from udpipe import UDPipe
import os
from dotenv import load_dotenv
from tabulate import tabulate
import re
import traceback


def gen_message(message, error=True):
    err_symb = '!' if error else ''
    return [f'{err_symb}<{message}>'] * len(UDPipe.Models) * 4


sheet_to_write = 'UDPipe!B1:B1'


print('begin')
ud = UDPipe()
load_dotenv()

gs = GoogleSheets(os.getenv('SHEET'))
lines = gs.read('data!C2:C')
lines = [item for sublist in lines for item in sublist]
print('GoogleSheets read ok')

use_udpipe = True
i = 0
data = []
len_lines = len(lines) - 1
for line in lines:
    line_fixed = line
    line_fixed = line_fixed.replace('-', ' ')
    line_fixed = re.sub(r'\d+([\.,]\d+)?\s*(см|кг|мл|шт|гр|л|лит|уп|%)', '', line_fixed, flags=re.IGNORECASE)
    line_fixed = re.sub(r'\d+([\.,]\d+)?\s*(г)', '', line_fixed, flags=re.IGNORECASE)
    line_fixed = re.sub(r'\s+(([a-zа-я]|\d)+\/)+([a-zа-я]|\d)+\s*', ' ', line_fixed, flags=re.IGNORECASE)
    line_fixed = re.sub(r'\s*вес\s*', '', line_fixed, flags=re.IGNORECASE)
    line_fixed = re.sub(r'[()\.+/|\\:]', '', line_fixed, flags=re.IGNORECASE)
    line_fixed = re.sub(r'\s*-\s*', '', line_fixed, flags=re.IGNORECASE)
    line_fixed = re.sub(r'\d+', '', line_fixed, flags=re.IGNORECASE)
    line_fixed = re.sub(r'^\s*[*]\s+', '', line_fixed, flags=re.IGNORECASE)
    line_fixed = line_fixed.replace('  ', ' ')
    line_fixed = line_fixed.strip()
    # line_fixed = line_fixed.lower()
    models = []
    try:
        if use_udpipe:
            for model in UDPipe.Models:
                df = ud.process_text(line_fixed, model)
                table = tabulate(df, headers='keys', tablefmt='simple')
                # root = df.loc[df['DepRel'] == 'root'].Lemma.item()
                root_df = df.loc[df['DepRel'] == 'root']
                if len(root_df.Lemma) == 0:
                    root = '!<not found>'

                    nmod_s = '!<root not found>'
                    continue

                if len(root_df.Lemma) > 1:
                    root = '!<root > 1>'
                    nmod_s = '!<root > 1>'
                    continue

                root = root_df.Form.item()
                root_lemma = root_df.Lemma.item()
                root_index = root_df.index.values[0]
                # print(root)
                # print(root_index)
                # print(table)
                nmod_df = df.loc[(df['DepRel'] == 'nmod') & (df['Head'] == str(root_index))]
                # print(tabulate(nmod_df, headers='keys', tablefmt='simple'))
                # print(nmod_df['Lemma'].values)
                nmod = nmod_df['Lemma'].values
                nmod_s = ' '.join([str(elem) for elem in nmod])
                models.extend([root, root_lemma, nmod_s, table])

        else:
            models = gen_message('-', False)

        print(f'processing {i}/{len_lines} "{line}" ok')

    except requests.exceptions.ReadTimeout as ex:
        print(f'processing {line} failed because of timeout: {str(ex)}')
        models = gen_message('UDPipe timeout')

    except HttpError as ex:
        print(f'processing {line} failed: {str(ex)}\n{ex.content}\n{ex.uri}\n{ex.error_details}')

    except Exception as ex:
        ex_test = traceback.format_exc()
        print(f'processing {line} failed: {str(ex)}; {ex_test}')

    line = [line, line_fixed]
    line.extend(models)
    data.append(line)

    i += 1
    if i % 10 == 0:
        gs.write(sheet_to_write, data)
        print('----------google sheet ok----------')
        data = []

gs.write(sheet_to_write, data)
print('loop ok')

print('GoogleSheets write ok')
print('fin')

