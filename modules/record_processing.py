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


def get_strings(row, column_names, concat):
    s = []
    for column_name in column_names:
        v = row[column_name]
        if isinstance(v, str):
            s.append(remove_double_letters(v))
        else:
            s.append(None)
    if concat:
        s = ["".join(s)]
    return s
