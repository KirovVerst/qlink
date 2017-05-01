import os
import re
import pandas as pd
from miac_experiments import MIAC_PATH

i = 5
PATH_RAW_DATA = os.path.join(MIAC_PATH, 'father_name_{}.csv'.format(i))
PATH_DATA = os.path.join(MIAC_PATH, 'father_name_{}.csv'.format(i + 1))
NUM_REGEX = re.compile('^[0-9]')
SYMBOLS_REGEX = re.compile('^[=,]')

soft = 'йьуеаоэяию'


# удалить пробелы 0
# удалить цифры 1
# удалить = ,
# в нижний регистр
# оглы -> ович, кызы -> овна. мягкое окончание : йьуеаоэяию
# удалить .
# заменить е на ё

def clean(s):
    if type(s) is str:
        r = s.replace('ё', 'е')
        return r
    else:
        return s


def asian_father_name_processing(s):
    if type(s) is str:
        pos = s.find('оглы')
        if pos > -1:
            s = s[:pos]
            s = s.strip(' -')
            if len(s) > 0 and s[-1] in soft:
                s += 'евич'
            else:
                s += 'ович'
        pos = s.find('кызы')
        if pos > -1:
            s = s[:pos]
            s = s.strip(' -')
            if len(s) > 0 and s[-1] in soft:
                s += 'евна'
            else:
                s += 'овна'
        return s
    else:
        return s


def active_fixing(dataframe):
    n = len(dataframe)
    for i in range(n):
        print('{}/{}'.format(i + 1, n))
        curr_value = dataframe.iloc[i]['father_name']
        print('Current value: {}'.format(curr_value))
        print('New value:')
        new_value = input()
        dataframe.iloc[i]['father_name'] = new_value
    return dataframe


if __name__ == '__main__':
    df = pd.read_csv(PATH_RAW_DATA)
    df = df[df['father_name'].str.contains(' ')]
    df = active_fixing(df)
    df.to_csv(PATH_DATA)
