import pandas as pd, numpy as np
import os, datetime, json

from pathos.multiprocessing import Pool

from collections import defaultdict
from conf import BASE_DIR
from modules.dataset_processing import EditDistanceMatrix

MIAC_SMALL_FOLDER = os.path.join(BASE_DIR, 'data', 'miac', 'small')
MIAC_SMALL_DATA = {
    'data': os.path.join(MIAC_SMALL_FOLDER, 'data.csv'),
    'sample': {
        'data': os.path.join(MIAC_SMALL_FOLDER, 'data-sample.csv'),
        'matrix': os.path.join(MIAC_SMALL_FOLDER, 'matrix-sample.json'),
        'index': os.path.join(MIAC_SMALL_FOLDER, 'index-sample.json'),
        'norm-matrix': os.path.join(MIAC_SMALL_FOLDER, 'norm-matrix-sample.json')
    }
}

STR_FIELDS = ['first_name', 'last_name', 'father_name']
DATE_FIELDS = ['birthday']
FIELDS = STR_FIELDS + DATE_FIELDS


def start_message(step_name):
    s = datetime.datetime.now()
    print('-' * 80)
    print(step_name)
    print('Start: ', s)
    return s


def finish_message(start_time):
    finish_time = datetime.datetime.now()
    print('Finish: ', finish_time)
    print('Delta: ', finish_time - start_time)
    print('-' * 80)
    return finish_time


def rearrange_list(values, n):
    r = len(values) % n
    if r > 0:
        rearranged_values = np.reshape(values[:-r], (-1, n))
    else:
        rearranged_values = np.reshape(values, (-1, n))
    rearranged_values = rearranged_values.transpose()
    rearranged_values = rearranged_values.tolist()
    if r > 0:
        rearranged_values[0] += values[-r:].tolist()
    return rearranged_values


def merge_dicts(dicts):
    result_dict = dict()
    for one_dict in dicts:
        result_dict.update(one_dict)
    return result_dict


class Indexation:
    def __init__(self, dataframe, index_field, index_output_path):
        self.dataframe = dataframe
        self.field = index_field
        self.output_path_json = index_output_path

    def _pool_function(self, values):
        i = 0
        index_dict = defaultdict(list)
        for value in values:
            value_length = len(value)
            df_sub = self.dataframe[self.dataframe[self.field].str.len() <= 3 * value_length]
            df_sub = df_sub[df_sub[self.field].str.contains(value, na=False)]
            index_dict[value] = list(map(lambda x: int(x), df_sub.index.tolist()))
            i += 1
            if i % 500 == 0:
                print(os.getpid(), ' : ', i, ' : ', datetime.datetime.now())
        return index_dict

    def create_index_dict(self):
        unique_values = np.unique(self.dataframe[self.field].values)
        values = rearrange_list(unique_values, os.cpu_count())

        s = start_message('Indexation')
        print('Unique values: ', len(unique_values))

        with Pool() as p:
            results = p.map(self._pool_function, values)

        index_dict = merge_dicts(results)

        with open(self.output_path_json, 'w') as fp:
            json.dump(index_dict, fp, ensure_ascii=False)

        finish_message(s)


class MatrixCalculation:
    def __init__(self, dataframe, index_path, matrix_output_path):
        self.dataframe = dataframe
        self.index_path = index_path
        self.output_path = matrix_output_path

    def create_matrix(self):
        s = start_message('Matrix')
        matrix_generator = EditDistanceMatrix(self.dataframe, str_column_names=STR_FIELDS, index_path=self.index_path,
                                              date_column_names=DATE_FIELDS, normalize=None)
        matrix = matrix_generator.get()
        with open(self.output_path, 'w') as fp:
            json.dump(matrix, fp)
        finish_message(s)
