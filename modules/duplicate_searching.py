import numpy as np
import os
from pathos.multiprocessing import Pool


class Predictor:
    def __init__(self, data, levels, list2float, comparator):
        """
        Class that provides a duplicate prediction in the dataset
        :param data: [[float, float, ...], ... ]. Matrix that shape is (n, n).
        :param levels: [[float, float, ...], ... ]. 
        :param list2float: str. Name of a function that converts list to float: "sum", "norm".
        :param comparator: str. Name of a comparision function: "or", "and".
        """
        self.data = data
        self.state = list(map(lambda level: dict(level=level, processed=set()), levels))
        self.list2float = self._list2float_norm if list2float == "norm" else self._list2float_sum
        self.comparator = self._comparator_and if comparator == "and" else self._comparator_or

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
        return sum(values > levels) == length

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
        for i in range(len(self.data)):
            if i not in self.state[state_index]['processed']:
                duplicates = [i] + self.recursive_search(i, state_index)
                items.append(duplicates)
        return dict(level=self.state[state_index]['level'], items=items)

    def recursive_search(self, row_index, state_index):
        """
        Searching duplicates of the record among all dataset for the selected state.
        :param row_index: index of record
        :param state_index: number of state
        :return: [int, int, ...]
        """
        self.state[state_index]['processed'].add(row_index)
        max_index = -1
        max_distance = 0
        levels = self.state[state_index]['level']
        for (j, e) in enumerate(self.data[row_index]):
            if self.comparator(values=e, levels=levels) and j not in self.state[state_index]['processed']:
                distance = self.list2float(e)
                if distance > max_distance:
                    max_distance = distance
                    max_index = j

        if max_index != -1:
            return [max_index] + self.recursive_search(max_index, state_index)
        else:
            return []

    def predict_duplicates(self, njobs=-1):
        """
        Searching duplicates for all states.
        :param njobs: 
        :return: [
            {
                "level": [float,],
                "items": [ [int, int, ...], [int, int, int, ...]]
            },   
        ]
        """

        if njobs <= -1 or njobs > os.cpu_count():
            njobs = os.cpu_count()
        with Pool(njobs) as p:
            results = list(p.map(self._predict_for_one_level, range(len(self.state))))

        return results
