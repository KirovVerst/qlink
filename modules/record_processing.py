import itertools


def remove_double_letters(s):
    """
    Removing double letters from the string
    :param s: str
    :return: str
    """
    group = itertools.groupby(s)
    res = ''.join(c for c, _ in group)
    return res


def get_strings(row, column_names):
    """
    Split row into list of values. Each value is a list of strings.
    :param row: 
    :param column_names: 
    :return: [['first_soname', 'second_soname'], ['name'], ['birthday']]
    """
    values_list = []
    for column_name in column_names:
        s = row[column_name]
        if isinstance(s, str) and s != 'nan':
            s = s.split('-')
            s = list(map(remove_double_letters, s))
            values_list.append(s)
        else:
            values_list.append([None])
    return values_list
