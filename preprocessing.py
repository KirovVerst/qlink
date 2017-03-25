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
