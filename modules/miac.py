import pandas as pd, numpy as np
import os, datetime, json

from pathos.multiprocessing import Pool

from collections import defaultdict
from conf import BASE_DIR
from modules.dataset_processing import EditDistanceMatrix
from modules.duplicate_searching import Predictor

MIAC_SMALL_FOLDER = os.path.join(BASE_DIR, 'data', 'miac', 'small')
MIAC_SMALL_DATA = {
    'sample': {
        'data': os.path.join(MIAC_SMALL_FOLDER, 'data-sample.csv'),
        'matrix': os.path.join(MIAC_SMALL_FOLDER, 'matrix-sample.json'),
        'index': os.path.join(MIAC_SMALL_FOLDER, 'index-sample.json'),
        'norm-matrix': os.path.join(MIAC_SMALL_FOLDER, 'norm-matrix-sample.json'),
        'duplicates': os.path.join(MIAC_SMALL_FOLDER, 'duplicates-sample.json')
    },
    'full': {
        'data': os.path.join(MIAC_SMALL_FOLDER, 'data.csv'),
        'matrix': os.path.join(MIAC_SMALL_FOLDER, 'matrix.json'),
        'index': os.path.join(MIAC_SMALL_FOLDER, 'index.json'),
        'norm-matrix': os.path.join(MIAC_SMALL_FOLDER, 'norm-matrix.json'),
        'duplicates': os.path.join(MIAC_SMALL_FOLDER, 'duplicates.json')
    }
}

STR_FIELDS = ['first_name', 'last_name', 'father_name']
DATE_FIELDS = ['birthday']
FIELDS = STR_FIELDS + DATE_FIELDS


def row_to_str(row):
    r = ''
    for field in STR_FIELDS + DATE_FIELDS:
        r += str(row[field]) + ' '
    return r


def get_lengths(row):
    lengths = []
    for field in FIELDS:
        if isinstance(row[field], str):
            lengths.append(len(row[field]))
        else:
            lengths.append(None)
    return lengths


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
        if not isinstance(values, list):
            values = values.tolist()
        rearranged_values[0] += values[-r:]
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

    def create_matrix(self, norm_matrix_path=None):
        s = start_message('Matrix')
        matrix_generator = EditDistanceMatrix(self.dataframe, str_column_names=STR_FIELDS, index_path=self.index_path,
                                              date_column_names=DATE_FIELDS, normalize='sum')
        matrix = matrix_generator.get()
        with open(self.output_path, 'w') as fp:
            json.dump(matrix, fp)

        if norm_matrix_path is not None:
            matrix = matrix['values']
            with open(norm_matrix_path, 'w') as fp:
                json.dump(matrix, fp)

        finish_message(s)


class Normalization:
    def __init__(self, dataframe, matrix_path, norm_matrix_output_path):
        self.dataframe = dataframe
        with open(matrix_path, 'r') as f:
            matrix = json.load(f)
            self.matrix = matrix['values']
            self.max_dist = matrix['max_dist']
        self.output_path = norm_matrix_output_path

    @staticmethod
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

    def _pool_function(self, keys):
        print("Process: ", os.getpid())
        count = 0
        result = defaultdict(list)
        for i in keys:
            length_i = get_lengths(self.dataframe.loc[int(i)])
            for j, dist in self.matrix[i]:
                length_j = get_lengths(self.dataframe.loc[j])
                norm_dist = Normalization.normalize(length_i, length_j, dist)
                result[i].append([j, norm_dist])
            count += 1
            if count % 100 == 0:
                print(os.getpid(), " : ", count)
        return result

    def create_norm_matrix(self):
        s = start_message('Normalization')

        keys = list(self.matrix.keys())
        print('Keys: ', len(keys))
        rearranged_keys = rearrange_list(keys, os.cpu_count())

        with Pool() as p:
            results = p.map(self._pool_function, rearranged_keys)

        norm_matrix = merge_dicts(results)

        with open(self.output_path, 'w') as fp:
            json.dump(norm_matrix, fp)

        finish_message(s)


class DuplicateSearching:
    def __init__(self, dataframe, norm_matrix_path, duplicates_output_path, mode):
        self.dataframe = dataframe
        with open(norm_matrix_path, 'r') as f:
            self.matrix = json.load(f)
        self.output_path = duplicates_output_path
        self.mode = mode

    def search_duplicates(self, level):
        s = start_message('Duplicate searching')

        predictor = Predictor(data=self.matrix, levels=[[level] * len(FIELDS)], list2float='norm', comparator='and',
                              save_extra_data=False, mode=self.mode)
        duplicates_list = predictor.predict_duplicates(njobs=1)
        extended_duplicates_list = []
        for duplicates in duplicates_list:
            items = duplicates['items']
            for keys in items:
                keys = list(set(map(lambda x: int(x), keys)))
                info = dict()
                for key in keys:
                    info[key] = row_to_str(self.dataframe.loc[key])
                extended_duplicates_list.append(info)

        duplicates_list[0]['items'] = extended_duplicates_list
        with open(self.output_path, 'w') as fp:
            json.dump(duplicates_list, fp, ensure_ascii=False)

        finish_message(s)
