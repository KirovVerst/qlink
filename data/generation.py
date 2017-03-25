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


def add_mistakes(x, columns, fracs):
    """

    :param x: pandas dataframe
    :param columns: list column names. ["first_name", "second_name"]
    :param fracs: list of fractions. Fractions' and columns' lists must have the same length. [0.1, 0.05]
    :return: pandas dataframe
    """
    n = len(x)
    counts = [ceil(x * n) for x in fracs]
    for i in range(len(columns)):
        ex = x.head(counts[i])
        ex[columns[i]] = ex[columns[i]].apply(make_random_mistake)
        x = pd.concat([ex, x.iloc[counts[i]:]])
        x = x.sample(frac=1)
    return x


data = pd.read_csv('original/data_1.csv')
N = 150
data = data.sample(N)
extended_data = duplicate_rows(data, frac=0.3)

extended_data.to_csv('extended/data_{0}.csv'.format(N), index=False)

fracs = [0.3, 0.3]
columns = ["first_name", "last_name"]
changed_data = add_mistakes(extended_data, columns, fracs)

changed_data.to_csv('ready/data_{0}.csv'.format(N), index=False)

"""
Duplicates search
"""
changed_data = changed_data.reset_index(drop=True)
truth = {}
for i, row in changed_data.iterrows():
    original_id = row['original_id']
    if original_id in truth:
        truth[original_id].append(i)
    else:
        truth[original_id] = [i]

with open('true_duplicates/data_{0}.txt'.format(N), 'w') as f:
    for k, v in truth.items():
        f.write(str(v) + '\n')
