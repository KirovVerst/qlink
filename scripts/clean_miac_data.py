import pandas as pd
import os
import datetime
import re
from miac_experiments import MIAC_PATH
from miac_experiments import DataPreprocessing

i = 27

RAW_PATH = os.path.join(MIAC_PATH, 'small', 'data.csv')
PATH = os.path.join(MIAC_PATH, 'big', 'data-27.csv')
TEMP_PATH = os.path.join(MIAC_PATH, 'temp.csv')


# 1. to lower
# 2. удалить все nan
# 3. оглы -> ович
# 4. удалить ,
# 5. пробелы в имени
# 6. двойные и девичьи фамилии
# 7. пробелы в фамилиях
# 8. пробелы во всех полях
# 9. эль-, -аль
# 10. удалить -
# 11. удалить .
def to_lower(dataframe):
    processor = DataPreprocessing(dataframe)
    processor.to_lower()
    return processor.dataframe


def dropnan(dataframe):
    dataframe.dropna(axis='index', how='all', inplace=True)
    return dataframe


soft = 'йьуеаоэяию'

count = 0


def asian_father_name_processing(s):
    global count
    if type(s) is str:
        print_res = s + ' --> '
        pos = s.find('оглы')
        if pos > -1:
            count += 1
            s = s[:pos]
            s = s.strip(' -')
            if len(s) > 0 and s[-1] in soft:
                s += 'евич'
            else:
                s += 'ович'
            print_res += s
            print(print_res)
        else:
            pos = s.find('кызы')
            if pos > -1:
                count += 1
                s = s[:pos]
                s = s.strip(' -')
                if len(s) > 0 and s[-1] in soft:
                    s += 'евна'
                else:
                    s += 'овна'
                print_res += s
                print(print_res)
        return s
    else:
        return s


def asian_father_name_column_processing(dataframe):
    dataframe['father_name'] = dataframe['father_name'].apply(asian_father_name_processing)
    return dataframe


def active_fixing(dataframe, fields):
    n = len(dataframe)
    print('Fixing columns: ', ' '.join(fields))
    for i in range(n):
        print('{}/{}'.format(i + 1, n))
        print('Row: ', dataframe.iloc[i].to_string())
        for field in fields:
            curr_value = dataframe.iloc[i][field]
            print('Current {}: {}'.format(field, curr_value))
            print('New value: ')
            new_value = input()
            if new_value == 'skip':
                break
            dataframe.iloc[i][field] = new_value
    return dataframe


def split_last_name(s):
    s = s.strip('()')
    s = s.split('(')
    s = list(map(lambda w: w.strip(' '), s))
    return '|'.join(s)


def multiple_last_names(dataframe):
    dataframe['last_name'] = dataframe['last_name'].apply(split_last_name)
    return dataframe


def remove_spaces(dataframe, fields):
    df_sub = pd.DataFrame(columns=['first_name', 'last_name', 'father_name', 'birthday'])
    for field in fields:
        t = dataframe[dataframe[field] == '-']
        df_sub = df_sub.append(t)
    print(df_sub)
    df_sub = active_fixing(df_sub, fields)
    df_sub.to_csv(TEMP_PATH, index=True)
    return df_sub


if __name__ == '__main__':
    df = pd.read_csv(PATH, index_col='id')
    df = df[df['last_name'].str.contains('o', na=False)]
    print(df)
