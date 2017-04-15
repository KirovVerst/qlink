import numpy as np
import os
from pathos.multiprocessing import Pool


class Predictor:
    def __init__(self, data, levels, list2float, comparator, save_extra_data):
        """
        Class that provides a duplicate prediction in the dataset
        :param data: defaultdict. {k = [(j, [float, float, ...])]}.
        :param levels: [[float, float, ...], ... ]. 
        :param list2float: str. Name of a function that converts list to float: "sum", "norm".
        :param comparator: str. Name of a comparision function: "or", "and".
        :param save_extra_data: bool
        """
        self.data = data
        self.state = list(map(lambda level: dict(level=level, processed=set()), levels))
        self.list2float = self._list2float_norm if list2float == "norm" else self._list2float_sum
        self.comparator = self._comparator_and if comparator == "and" else self._comparator_or
        self._save_extra_data = save_extra_data
        if self._save_extra_data:
            self.extra_data = [dict()] * len(levels)

    @staticmethod
    def _list2float_norm(values):
        return np.linalg.norm(values)

    @staticmethod
    def _list2float_sum(values):
        return sum(values)

    @staticmethod
    def _comparator_and(values, levels):
        """
        If all values are bigger than their levels, the function returns True.
        :param values: [float]. List of calculated distance
        :param levels: [float]. List of levels
        :return: bool
        """
        length = len(values)
        return sum(np.array(values) > levels) == length

    @staticmethod
    def _comparator_or(values, levels):
        """
        If at least one of values is bigger than its level, the function returns True.
        :param values: [float]. List of calculated distance
        :param levels: [float]. List of levels
        :return: bool
        :return: 
        """
        return sum(values > levels) >= 1

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
        for i in self.data:
            if i not in self.state[state_index]['processed']:
                duplicates = [i] + self.recursive_search(i, state_index, extra_data)
                items.append(duplicates)
        return dict(level=self.state[state_index]['level'], items=items, extra_data=extra_data)

    def recursive_search(self, row_id, state_index, extra_data):
        """
        Searching duplicates of the record among all dataset for the selected state.
        :param row_id: index of record
        :param state_index: number of state
        :return: [int, int, ...]
        """
        self.state[state_index]['processed'].add(row_id)
        levels = self.state[state_index]['level']

        records = list(filter(lambda x: self.comparator(values=x[1], levels=levels), self.data[row_id]))
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
