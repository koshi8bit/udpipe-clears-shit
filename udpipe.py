# https://lindat.mff.cuni.cz/services/udpipe/
# https://lindat.mff.cuni.cz/services/udpipe/api-reference.php
import pandas as pd
import requests
from enum import Enum


class UDPipe(object):
    """docstring"""

    class Models(Enum):
        default = 'rus'
        m1 = 'russian-syntagrus-ud-2.6-200830'
        m2 = 'russian-gsd-ud-2.6-200830'
        m3 = 'russian-taiga-ud-2.6-200830'

    def __init__(self):
        """Constructor"""
        pass

    def filter_lines(self, text):
        lines = text.split('\n')
        result = []
        for line in lines:
            if line.startswith('#') or line == '':
                continue
            result.append(line)

        return result

    def prepeare_pd(self, lines):
        headers = ['Form', 'Lemma', 'UPosTag', 'XPosTag', 'Feats', 'Head', 'DepRel', 'Deps', 'Misc']

        df = pd.DataFrame(columns=headers)
        df.loc[0] = [''] * len(headers)
        for line in lines:
            columns = line.split('\t')
            del columns[0]
            if len(columns) != len(headers):
                raise ValueError(f'Invalid line len ({len(columns)})')

            df_length = len(df)
            df.loc[df_length] = columns

        return df

    def process_text(self, text, model=Models.default):
        response = requests.get(
            f'http://lindat.mff.cuni.cz/services/udpipe/api/process?tokenizer&tagger&parser&data={text}'
            f'&model={model.value}',
            timeout=10)
        response_str = response.json()['result']

        lines = self.filter_lines(response_str)

        return self.prepeare_pd(lines)

