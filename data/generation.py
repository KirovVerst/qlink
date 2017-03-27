import pandas as pd
import random, os
from math import ceil


def duplicate_rows(data, rng=1, frac=0.0):
    l = [data]
    extra = data
    for i in range(rng):
        extra = extra.sample(frac=frac)
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


N = 1000
fracs = [0.3, 0.3, 0.3]
columns = ["first_name", "last_name", "father"]

data_folder_path = 'ready/{0}'.format(N)
if not os.path.exists(data_folder_path):
    os.mkdir(data_folder_path)

duplicates_folder_path = 'true_duplicates/{0}'.format(N)
if not os.path.exists(duplicates_folder_path):
    os.mkdir(duplicates_folder_path)

document_cnt = 0

for original_data_number in range(1):
    full_data = pd.read_csv('original/data_{0}.csv'.format(original_data_number))
    for i in range(2):

        data = full_data.sample(N)
        extended_data = duplicate_rows(data, rng=2, frac=0.3)

        changed_data = add_mistakes(extended_data, columns, fracs)

        full_path = os.path.join(data_folder_path, 'data_{0}.csv'.format(document_cnt))
        changed_data.to_csv(full_path, index=False)

        """
        Duplicates search
        """
        changed_data = changed_data.reset_index(drop=True)
        truth = {}
        for j, row in changed_data.iterrows():
            original_id = row['original_id']
            if original_id in truth:
                truth[original_id].append(j)
            else:
                truth[original_id] = [j]

        full_path = os.path.join(duplicates_folder_path, 'data_{0}.txt'.format(document_cnt))
        with open(full_path, 'w') as f:
            for k, v in truth.items():
                f.write(str(v) + '\n')

        document_cnt += 1
