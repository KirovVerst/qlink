import os
import pandas as pd
import json
from collections import defaultdict
from conf import BASE_DIR
from itertools import groupby

DATA_FOLDER = os.path.join(BASE_DIR, 'data', 'miac', 'test')
DATA_FOLDER_ORIGINAL = os.path.join(DATA_FOLDER, 'ready')
DATA_FOLDER_DUPLICATES = os.path.join(DATA_FOLDER, 'duplicates')


def get_dataset(document_number):
    path = os.path.join(DATA_FOLDER_ORIGINAL, 'data_{}.csv'.format(document_number))
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


def create_duplicates(document_number):
    dataset = get_dataset(document_number)
    duplicates = get_duplicates(dataset)
    json_duplicates = dict(items=list(duplicates.values()))
    with open(os.path.join(DATA_FOLDER_DUPLICATES, 'data_{}.json'.format(document_number)), 'w') as fp:
        json.dump(json_duplicates, fp, ensure_ascii=False)


def get_stats(document_number):
    dataset = get_dataset(document_number)
    l = dataset['original_id'].values.tolist()
    s = set(l)
    print('Total: ', len(l))
    print('Unique: ', len(s))
    result = {1: 0, 2: 0, 3: 0}
    for value in s:
        count = l.count(value)
        if count in result:
            result[count] += 1
        else:
            result[3] += 1
    print(result)


if __name__ == '__main__':
    create_duplicates(document_number=2)
