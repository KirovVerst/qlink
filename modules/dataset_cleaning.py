import re

NUM_REGEX = re.compile('^[0-9]')
SYMBOLS_REGEX = re.compile('^[=,]')

SOFT_LETTERS = 'йьуеаоэяию'


def asian_father_name_processing(s):
    if type(s) is str:
        pos = s.find('оглы')
        if pos > -1:
            s = s[:pos]
            s = s.strip(' -')
            if len(s) > 0 and s[-1] in SOFT_LETTERS:
                s += 'евич'
            else:
                s += 'ович'
        pos = s.find('кызы')
        if pos > -1:
            s = s[:pos]
            s = s.strip(' -')
            if len(s) > 0 and s[-1] in SOFT_LETTERS:
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


def dropnan(dataframe):
    dataframe.dropna(axis='index', how='all', inplace=True)
    return dataframe


def asian_father_name_column_processing(dataframe):
    dataframe['father_name'] = dataframe['father_name'].apply(asian_father_name_processing)
    return dataframe


def split_last_name(s):
    s = s.strip('()')
    s = s.split('(')
    s = list(map(lambda w: w.strip(' '), s))
    return '|'.join(s)


def multiple_last_names(dataframe):
    dataframe['last_name'] = dataframe['last_name'].apply(split_last_name)
    return dataframe
