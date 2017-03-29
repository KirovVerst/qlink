import pandas as pd
import random
from math import ceil


def duplicate_rows(data, rng=1, frac=0.0):
    l = [data]
    extra = data
    for i in range(rng):
        extra = extra.sample(frac=frac, random_state=42)
        l.append(extra)
    return pd.concat(l)


def duplicate_letter(s):
    position = random.randrange(len(s))
    return s[:position] + s[position] + s[position:]


def remove_letter(s):
    position = random.randint(0, len(s) - 1)
    return s[:position] + s[position + 1:]


def make_random_mistake(s):
    return duplicate_letter(s) if random.random() > 0.5 else remove_letter(s)


def add_mistakes(x, columns):
    """

    :param x: pandas dataframe
    :param columns: dict(column_name=0.1, column_name_2=0.4)
    :return: pandas dataframe
    """
    n = len(x)
    for k in columns:
        columns[k] = ceil(columns[k] * n)

    for column_name, count in columns.items():
        ex = x.head(count)
        ex[column_name] = ex[column_name].apply(make_random_mistake)
        x = pd.concat([ex, x.iloc[count:]])
        x = x.sample(frac=1)
    return x
