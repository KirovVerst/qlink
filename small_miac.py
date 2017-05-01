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

FOLDER_PATH = os.path.join(BASE_DIR, 'data', 'miac', 'small')
DATAFRAME_PATH = os.path.join(FOLDER_PATH, 'data.csv')
INDEX_PATH = os.path.join(FOLDER_PATH, 'index.json')
MATRIX_PATH = os.path.join(FOLDER_PATH, 'matrix.pkl')
MATRIX_PATH_JSON = os.path.join(FOLDER_PATH, 'matrix.json')

STR_FIELDS = ['first_name', 'last_name', 'father_name']
DATE_FIELDS = ['birthday']


def create_index_dict(df, field, index_path):
    values = np.unique(df[field].values)
    r = len(values) % os.cpu_count()
    list_values = np.reshape(values[:-r], (-1, os.cpu_count()))
    list_values = list_values.transpose()
    list_values = list_values.tolist()
    list_values[0] += values[-r:].tolist()
    print('-' * 80)
    s = datetime.datetime.now()
    print('Indexing')
    print('Start: ', s)

    def run(values):
        i = 0
        index_dict = defaultdict(list)
        for value in values:
            df_sub = df[df[field].str.contains(value, na=False)]
            index_dict[value] = list(map(lambda x: int(x), df_sub.index.tolist()))
            i += 1
            if i % 500 == 0:
                print(os.getpid(), ' : ', i, ' : ', datetime.datetime.now())
        return index_dict

    with Pool() as p:
        results = p.map(run, list_values)

    index_dict = defaultdict(list)
    for res in results:
        index_dict.update(res)

    with open(index_path, 'w') as fp:
        json.dump(index_dict, fp, ensure_ascii=False)
    e = datetime.datetime.now()
    print('Finish: ', e)
    print('Delta: ', e - s)
    print('-' * 80)


def matrix_calculation(df, index_path, matrix_path):
    s = datetime.datetime.now()
    print('-' * 80)
    print('Matrix')
    print('Start: ', s)
    matrix_generator = EditDistanceMatrix(df, str_column_names=STR_FIELDS, index_path=index_path,
                                          date_column_names=DATE_FIELDS, normalize=None)
    matrix = matrix_generator.get()
    with open(matrix_path, 'w') as fp:
        json.dump(matrix, fp)
    e = datetime.datetime.now()
    print('Finish: ', e)
    print('Delta: ', e - s)
    print('-' * 80)


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
    df = pd.read_csv(DATAFRAME_PATH, index_col='id')
    norm_matrix_path = os.path.join(FOLDER_PATH, 'norm-sum-matrix.json')
    duplicates_path = os.path.join(FOLDER_PATH, 'duplicates-sample.json')
