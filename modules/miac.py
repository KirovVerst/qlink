import pandas as pd, numpy as np
import os, datetime, json
import itertools
import copy

from pathos.multiprocessing import Pool
from collections import defaultdict
from conf import BASE_DIR
from modules.dataset_processing import EditDistanceMatrix
from modules.duplicate_searching import Predictor

MIAC_SMALL_FOLDER = os.path.join(BASE_DIR, 'data', 'miac', 'small')
MIAC_BIG_FOLDER = os.path.join(BASE_DIR, 'data', 'miac', 'big')

MIAC_SMALL_DATA = {
    'sample': {
        'data': os.path.join(MIAC_SMALL_FOLDER, 'data-sample.csv'),
        'matrix': os.path.join(MIAC_SMALL_FOLDER, 'matrix-sample.json'),
        'index-substr': os.path.join(MIAC_SMALL_FOLDER, 'index-sample-substr.json'),
        'index-letters': os.path.join(MIAC_SMALL_FOLDER, 'index-sample-letters.json'),
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

MIAC_BIG_DATA = {
    'sample': {
        'data': os.path.join(MIAC_BIG_FOLDER, 'data-sample.csv'),
        'matrix': os.path.join(MIAC_BIG_FOLDER, 'matrix-sample.json'),
        'index': os.path.join(MIAC_BIG_FOLDER, 'index-sample.json'),
        'norm-matrix': os.path.join(MIAC_BIG_FOLDER, 'norm-matrix-sample.json'),
        'duplicates': os.path.join(MIAC_BIG_FOLDER, 'duplicates-sample.json')
    },
    'full': {
        'data': os.path.join(MIAC_BIG_FOLDER, 'data.csv'),
        'matrix': os.path.join(MIAC_BIG_FOLDER, 'matrix.json'),
        'index': os.path.join(MIAC_BIG_FOLDER, 'index.json'),
        'norm-matrix': os.path.join(MIAC_BIG_FOLDER, 'norm-matrix.json'),
        'duplicates': os.path.join(MIAC_BIG_FOLDER, 'duplicates.json')
    }
}


def row_to_str(row, fields):
    fields.remove('original_id')
    r = ''
    for field in fields:
        r += str(row[field]) + ' '
    return r


def get_lengths(row, fields):
    fields.remove('original_id')
    lengths = []
    for field in fields:
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
    def __init__(self, dataframe, index_field, index_output_path, mode):
        """
        
        :param dataframe: 
        :param index_field: 
        :param index_output_path: 
        :param mode: ```substr```, ```letters```
        """
        self.dataframe = dataframe
        self.field = index_field
        self.output_path_json = index_output_path
        self.index_function = self._letters_filter if mode == "letters" else self._substr_filter
        self.level = 0.9

    @staticmethod
    def _check_common_letters(str1, set2, level):
        set1 = set(str1)
        common_part = len(set1.intersection(set2))

        return common_part / len(set2) >= level and common_part / len(set1) >= level

    def _substr_filter(self, dataframe, value):
        return dataframe[dataframe[self.field].str.contains(value, na=False)]

    def _letters_filter(self, dataframe, value):
        set2 = set(value)
        return dataframe[dataframe[self.field].apply(lambda x: Indexation._check_common_letters(x, set2, self.level))]

    def _pool_function(self, values):
        i = 0
        index_dict = defaultdict(list)
        for value in values:
            value_length = len(value)
            df_sub = self.dataframe[self.dataframe[self.field].str.len() <= 4 * value_length]
            df_sub = self.index_function(dataframe=df_sub, value=value)
            index_dict[value] = list(map(lambda x: int(x), df_sub.index.tolist()))
            i += 1
            if i % 500 == 0:
                print(os.getpid(), ' : ', i, ' : ', datetime.datetime.now())
        return index_dict

    def create_index_dict(self):
        values = self.dataframe[self.field].values
        values = list(map(lambda value: value.split('-'), values))
        unique_values = np.unique(list(itertools.chain(*values)))
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
    def __init__(self, dataframe, str_fields, date_fields, index_path, matrix_path, index_field, norm_matrix_path):
        self.dataframe = dataframe
        self.index_path = index_path
        self.matrix_path = matrix_path
        self.norm_matrix_path = norm_matrix_path
        self.index_field = index_field
        self.str_fields = str_fields
        self.date_fields = date_fields

    def create_matrix(self, njobs=-1):
        s = start_message('Matrix')
        matrix_generator = EditDistanceMatrix(dataframe=self.dataframe,
                                              str_column_names=self.str_fields, date_column_names=self.date_fields,
                                              index_path=self.index_path, index_field=self.index_field,
                                              normalize=True)
        matrix = matrix_generator.get(njobs)
        with open(self.matrix_path, 'w') as fp:
            json.dump(matrix, fp)

        matrix = matrix['values']
        with open(self.norm_matrix_path, 'w') as fp:
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
            length_i = get_lengths(self.dataframe.loc[int(i)], fields=self.dataframe.columns.values.tolist())
            for j, dist in self.matrix[i]:
                length_j = get_lengths(self.dataframe.loc[j], fields=self.dataframe.columns.values.tolist())
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
        self.duplicates_list = []

    def search_duplicates(self, level):
        if len(level) != len(self.dataframe.columns.values.tolist()):
            print('Level threshold is not correct. length = ', len(level))
        s = start_message('Duplicate searching')

        predictor = Predictor(data=self.matrix, levels=[level], list2float='norm', comparator='and',
                              save_extra_data=False, mode=self.mode)

        self.duplicates_list = predictor.predict_duplicates(njobs=1)
        json_duplicates_list = copy.deepcopy(self.duplicates_list)
        extended_duplicates_list = []

        for duplicates in json_duplicates_list:
            items = duplicates['items']
            for keys in items:
                keys = list(set(map(lambda x: int(x), keys)))
                info = dict()
                for key in keys:
                    info[key] = row_to_str(self.dataframe.loc[key], self.dataframe.columns.values.tolist())
                extended_duplicates_list.append(info)

        json_duplicates_list[0]['items'] = extended_duplicates_list
        with open(self.output_path, 'w') as fp:
            json.dump(json_duplicates_list, fp, ensure_ascii=False)

        finish_message(s)
