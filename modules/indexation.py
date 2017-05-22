import numpy as np
import datetime
import itertools
import os
import json

from modules.log_functions import start_message, finish_message
from modules.utils import merge_dicts, rearrange_list

from collections import defaultdict
from pathos.multiprocessing import Pool


class Indexation:
    MODE_LETTERS = 0
    MODE_SUBSTR = 1

    def __init__(self, dataframe, index_field, index_output_path, mode, level=0.9):
        """

        :param dataframe: 
        :param index_field: 
        :param index_output_path: 
        :param mode: 0, 1
        """
        self.dataframe = dataframe
        self.field = index_field
        self.output_path_json = index_output_path
        self.index_function = self._letters_filter if mode == Indexation.MODE_LETTERS else self._substr_filter
        self.level = level

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

    def create_index_dict(self, njobs):
        if njobs == -1:
            njobs = os.cpu_count()
        values = self.dataframe[self.field].values
        values = list(map(lambda value: value.split('-'), values))
        unique_values = np.unique(list(itertools.chain(*values)))
        values = rearrange_list(unique_values, njobs)

        s = start_message('Indexation')
        print('Unique values: ', len(unique_values))
        if njobs == 1:
            results = [self._pool_function(values[0])]
        else:
            with Pool() as p:
                results = p.map(self._pool_function, values)

        index_dict = merge_dicts(results)

        with open(self.output_path_json, 'w') as fp:
            json.dump(index_dict, fp, ensure_ascii=False)

        finish_message(s)
