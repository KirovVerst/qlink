import pandas as pd
import random, os, json
from math import ceil
from multiprocessing import Pool, current_process
from collections import defaultdict


def duplicate_rows(data, rng=1, frac=0.0):
    l = [data]
    extra = data
    for i in range(rng):
        extra = extra.sample(frac=frac, random_state=42)
        l.append(extra)
    return pd.concat(l)


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


N = 200
fracs = [0.3, 0.3, 0.3]
columns = ["first_name", "last_name", "father"]
DATASET_NUMBER_PER_DOCUMENT = 1
RAW_DOCUMENT_NUMBER = 12

data_folder_path = 'ready/{0}'.format(N)
if not os.path.exists(data_folder_path):
    os.mkdir(data_folder_path)

duplicates_folder_path = 'true_duplicates/{0}'.format(N)
if not os.path.exists(duplicates_folder_path):
    os.mkdir(duplicates_folder_path)


def func(dataset_id):
    raw_doc_id = dataset_id // DATASET_NUMBER_PER_DOCUMENT
    s = 'original/data_{0}.csv'.format(raw_doc_id)
    full_data = pd.read_csv(s)
    data = full_data.sample(N, random_state=dataset_id)
    extended_data = duplicate_rows(data, rng=2, frac=0.3)

    changed_data = add_mistakes(extended_data, columns, fracs)

    full_path = os.path.join(data_folder_path, 'data_{0}.csv'.format(dataset_id))
    changed_data.to_csv(full_path, index=False)

    """
    Duplicates search
    """
    changed_data = changed_data.reset_index(drop=True)
    truth = defaultdict(list)
    for j, row in changed_data.iterrows():
        truth[row['original_id']].append(int(j))

    full_path = os.path.join(duplicates_folder_path, 'data_{0}.json'.format(dataset_id))
    with open(full_path, 'w') as fp:
        json.dump(dict(values=list(truth.values())), fp)


with Pool(1) as p:
    p.map(func, list(range(RAW_DOCUMENT_NUMBER * DATASET_NUMBER_PER_DOCUMENT)))
