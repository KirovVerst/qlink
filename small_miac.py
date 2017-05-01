import os
import pandas as pd
import json
import numpy as np
import datetime
import _pickle as pickle
from collections import defaultdict
from pathos.multiprocessing import Pool

from conf import BASE_DIR
from modules.dataset_processing import EditDistanceMatrix
from duplicate_searching import Predictor

from modules.miac import MIAC_SMALL_DATA
from modules.miac import Indexation, MatrixCalculation

FOLDER_PATH = os.path.join(BASE_DIR, 'data', 'miac', 'small')
DATAFRAME_PATH = os.path.join(FOLDER_PATH, 'data.csv')
INDEX_PATH = os.path.join(FOLDER_PATH, 'index.json')
MATRIX_PATH = os.path.join(FOLDER_PATH, 'matrix.pkl')
MATRIX_PATH_JSON = os.path.join(FOLDER_PATH, 'matrix.json')

STR_FIELDS = ['first_name', 'last_name', 'father_name']
DATE_FIELDS = ['birthday']


def get_length(row):
    r = []
    fields = DATE_FIELDS + STR_FIELDS
    for field in fields:
        if isinstance(row[field], str):
            r.append(len(row[field]))
        else:
            r.append(None)
    return r


def normalize(length_1, length_2, dist):
    k = len(length_1)
    result = []
    for i in range(k):
        if length_1[i] is None or length_2[i] is None or dist[i] is None:
            result.append(None)
        else:
            s = length_1[i] + length_2[i]
            result.append((s - dist[i]) / s)
    return result


def normalization(matrix_path, norm_matrix_path):
    print('-' * 80)
    s = datetime.datetime.now()
    print('Normalization')
    print('Start: ', s)
    with open(matrix_path, 'r') as fp:
        matrix = json.load(fp)
    max_dist = matrix['max_dist']
    matrix = matrix['values']

    keys = list(matrix.keys())
    print('Keys: ', len(keys))
    r = len(keys) % os.cpu_count()
    if r > 0:
        list_keys = np.reshape(keys[:-r], (-1, os.cpu_count()))
    else:
        list_keys = np.reshape(keys, (-1, os.cpu_count()))
    list_keys = list_keys.transpose()
    list_keys = list_keys.tolist()
    if r > 0:
        list_keys[0] += keys[-r:]

    def run(index):
        print("Process: ", os.getpid())
        count = 0
        local_result = defaultdict(list)
        for i in list_keys[index]:
            length_i = get_length(df.loc[int(i)])
            for j, dist in matrix[i]:
                length_j = get_length(df.loc[j])
                norm_dist = normalize(length_i, length_j, dist)
                local_result[i].append([j, norm_dist])
            count += 1
            if count % 1000 == 0:
                print(os.getpid(), " : ", count)
        return local_result

    with Pool() as p:
        results = p.map(run, range(len(list_keys)))

    norm_matrix = dict()
    for result in results:
        norm_matrix.update(result)

    with open(norm_matrix_path, 'w') as fp:
        json.dump(norm_matrix, fp)


def row_to_str(row):
    r = ''
    for field in STR_FIELDS + DATE_FIELDS:
        r += str(row[field]) + ' '
    return r


def search_duplicates(norm_matrix_path, duplicates_path):
    with open(norm_matrix_path, 'r') as fp:
        matrix = json.load(fp)
    predictor = Predictor(data=matrix, levels=[[1] * 4],
                          list2float='sum', comparator='and', save_extra_data=False)
    duplicates = predictor.predict_duplicates(njobs=1)
    new_duplicates_items = []
    for duplicates_one_level in duplicates:
        items = duplicates_one_level['items']
        for ids in items:
            ids = list(set(map(lambda x: int(x), ids)))
            info = dict()
            for index in ids:
                info[index] = row_to_str(df.loc[index])

            new_duplicates_items.append(info)
    duplicates[0]['items'] = new_duplicates_items
    with open(duplicates_path, 'w') as fp:
        json.dump(duplicates, fp, ensure_ascii=False)


if __name__ == '__main__':
    df = pd.read_csv(MIAC_SMALL_DATA['sample']['data'], index_col='id')
    matrix_calculator = MatrixCalculation(df, index_path=MIAC_SMALL_DATA['sample']['index'],
                                          matrix_output_path=MIAC_SMALL_DATA['sample']['matrix'])
    matrix_calculator.create_matrix()
