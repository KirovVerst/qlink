import pandas as pd
import random
from math import ceil


def duplicate_rows(data, frac=0.0):
    extra = data.sample(frac=frac)
    return pd.concat([data, extra])


def add_letter(s):
    position = random.randrange(len(s))
    return s[:position] + s[position] + s[position:]


def remove_letter(s):
    position = random.randint(0, len(s) - 1)
    return s[:position] + s[position + 1:]


def make_random_mistake(s):
    return add_letter(s) if random.random() > 0.5 else remove_letter(s)


def add_mistakes(X, columns, fracs):
    """

    :param X: pandas dataframe
    :param columns: list column names. ["first_name", "second_name"]
    :param fracs: list of fractions. Fractions' and columns' lists must have the same length. [0.1, 0.05]
    :return: pandas dataframe
    """
    n = len(X)
    counts = [ceil(x * n) for x in fracs]
    for i in range(len(columns)):
        ex = X.head(counts[i])
        ex[columns[i]] = ex[columns[i]].apply(make_random_mistake)
        X = pd.concat([ex, X.iloc[counts[i]:]])
        X = X.sample(frac=1)
    return X


"""
data = pd.read_csv('original/data_1.csv')
ex_example = duplicate_rows(data, frac=0.3)

ex_example.to_csv('extended/data_1.csv', index=False)
"""

data = pd.read_csv('extended/data_1.csv')

fracs = [0.3, 0.3]
columns = ["first_name", "last_name"]
changed_data = add_mistakes(data, columns, fracs)

changed_data.to_csv('ready/data_1.csv', index=False)
