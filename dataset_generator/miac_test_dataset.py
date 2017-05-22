import os
import pandas as pd
import json
from collections import defaultdict
from conf import BASE_DIR
from itertools import groupby

DATA_FOLDER = os.path.join(BASE_DIR, 'data', 'miac', 'test')
DATA_FOLDER_ORIGINAL = os.path.join(DATA_FOLDER, 'ready')
DATA_FOLDER_DUPLICATES = os.path.join(DATA_FOLDER, 'duplicates')


def get_dataset(i):
    path = os.path.join(DATA_FOLDER_ORIGINAL, 'data_{}.csv'.format(i))
    dataset = pd.read_csv(path)
    return dataset


def get_duplicates(dataset):
    """
    
    :param dataset: 
    :return: {1=[1,2,3,4,5], }
    """
    duplicates = defaultdict(list)
    for j, row in dataset.iterrows():
        duplicates[row['original_id']].append(int(j))
    return duplicates


def create_duplicates():
    dataset = get_dataset(i=0)
    duplicates = get_duplicates(dataset)
    json_duplicates = dict(items=list(duplicates.values()))
    with open(os.path.join(DATA_FOLDER_DUPLICATES, 'data_{}.json'.format(0)), 'w') as fp:
        json.dump(json_duplicates, fp, ensure_ascii=False)


def get_stats(i):
    dataset = get_dataset(i)
    ids = dataset['original_id'].values.tolist()
    print('Total: ', len(ids))
    print('Unique: ', len(set(ids)))
    g = groupby(ids)
    duplicates = []
    for k, v in g:
        t = list(v)
        if len(t) > 1:
            duplicates.append(t)
    for v in duplicates:
        print(list(v))
# 9, 19
# 981 + 9 = 990


if __name__ == '__main__':
    get_stats(i=0)
