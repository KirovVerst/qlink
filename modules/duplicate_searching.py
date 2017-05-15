import numpy as np
import os
import json
import copy

from pathos.multiprocessing import Pool
from log_functions import start_message, finish_message


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


class Searcher:
    def __init__(self, data, levels, list2float, comparator, save_extra_data, mode):
        """
        Class that provides a duplicate prediction in the dataset
        :param data: defaultdict. {k = [(j, [float, float, ...])]}.
        :param levels: [[float, float, ...], ... ]. 
        :param list2float: str. Name of a function that converts list to float: "sum", "norm".
        :param comparator: str. Name of a comparision function: "or", "and".
        :param save_extra_data: bool
        :param mode: ```max```, ```all```
        """
        self.data = data
        self.state = list(map(lambda level: dict(level=level, processed=set()), levels))
        self.list2float = self._list2float_norm if list2float == "norm" else self._list2float_sum
        self.comparator = self._comparator_and if comparator == "and" else self._comparator_or
        self._save_extra_data = save_extra_data
        self.mode = mode
        if self._save_extra_data:
            self.extra_data = [dict()] * len(levels)

    @staticmethod
    def _list2float_norm(values):
        values = list(filter(lambda x: x is not None, values))
        return np.linalg.norm(values)

    @staticmethod
    def _list2float_sum(values):
        values = list(filter(lambda x: x is not None, values))
        return sum(values)

    @staticmethod
    def _comparator_and(values, levels):
        """
        If all values are bigger than their levels, the function returns True.
        :param values: [float]. List of calculated distance
        :param levels: [float]. List of levels
        :return: bool
        """
        total_count = 0
        count = 0
        for i, value in enumerate(values):
            if value is None:
                continue
            total_count += 1
            if value >= levels[i]:
                count += 1
        return total_count == count

    @staticmethod
    def _comparator_or(values, levels):
        """
        If at least one of values is bigger than its level, the function returns True.
        :param values: [float]. List of calculated distance
        :param levels: [float]. List of levels
        :return: bool
        :return: 
        """
        for i, value in enumerate(values):
            if value is None:
                continue
            if value >= levels[i]:
                return True
        return False

    def _predict_for_one_level(self, state_index):
        """
        Searching all duplicates for the selected state.
        :param state_index: int. Number of state that 
        :return: 
        {
            "level": [float,],
            "items": [ [int, int, ...], [int, int, int, ...]]
        }
        """

        items = []
        extra_data = dict()
        count = 0
        print('Keys: ', len(self.data))
        for i in self.data:
            count += 1
            if i not in self.state[state_index]['processed']:

                if self.mode == 'all':
                    duplicates = self.all_acceptable_search(i, state_index, extra_data)
                elif self.mode == 'max':
                    duplicates = [i] + self.recursive_search(i, state_index, extra_data)

                if len(duplicates) > 0:
                    items.append(list(duplicates))
                else:
                    items.append([i])

            if count % 1000 == 0:
                print('Processed: ', count)

        return dict(level=self.state[state_index]['level'], items=items, extra_data=extra_data)

    def all_acceptable_search(self, row_id, state_index, extra_data):
        levels = self.state[state_index]['level']
        records = list(filter(lambda x: self.comparator(values=x[1], levels=levels), self.data[str(row_id)]))
        records = list(filter(lambda x: str(x[0]) not in self.state[state_index]['processed'], records))
        keys = set(map(lambda x: str(x[0]), records))
        if len(keys) > 0:
            keys.add(str(row_id))
            self.state[state_index]['processed'] = self.state[state_index]['processed'].union(keys)
        return keys

    def recursive_search(self, row_id, state_index, extra_data):
        """
        Searching duplicates of the record among all dataset for the selected state.
        :param row_id: index of record
        :param state_index: number of state
        :return: [int, int, ...]
        """
        self.state[state_index]['processed'].add(row_id)
        levels = self.state[state_index]['level']
        try:
            records = list(filter(lambda x: self.comparator(values=x[1], levels=levels), self.data[str(row_id)]))
            records = list(filter(lambda x: x[0] not in self.state[state_index]['processed'], records))
            records = list(map(lambda x: (x[0], x[1], self.list2float(x[1])), records))

            if len(records) > 0:
                max_record = max(records, key=lambda x: x[2])
                if self._save_extra_data:
                    extra_data_item = dict(eval_levels=max_record[1], according=row_id)
                    extra_data[max_record[0]] = extra_data_item
                return [max_record[0]] + self.recursive_search(max_record[0], state_index, extra_data)
            else:
                return []
        except Exception as e:
            print(e.args)
            return []

    def predict_duplicates(self, njobs=-1):
        """
        Searching duplicates for all states.
        :param njobs: 
        :return: [
            {
                "level": [float,],
                "items": [ [int, int, ...], [int, int, int, ...]],
                "extra_data": dict()
            },   
        ]
        """

        if njobs <= -1 or njobs > os.cpu_count():
            njobs = os.cpu_count()
        if njobs == 1:
            results = list(map(self._predict_for_one_level, range(len(self.state))))
        else:
            with Pool(njobs) as p:
                results = list(p.map(self._predict_for_one_level, range(len(self.state))))
        return results


class DuplicateSearching:
    def __init__(self, dataframe, norm_matrix_path, duplicates_path, mode):
        self.dataframe = dataframe
        with open(norm_matrix_path, 'r') as f:
            self.matrix = json.load(f)
        self.output_path = duplicates_path
        self.mode = mode
        self.duplicates_list = []

    def search_duplicates(self, level):
        if len(level) != len(self.dataframe.columns.values.tolist()):
            print('Level threshold is not correct. length = ', len(level))
        s = start_message('Duplicate searching')

        predictor = Searcher(data=self.matrix, levels=[level], list2float='norm', comparator='and',
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
