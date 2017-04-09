from math import ceil
import numpy as np


class Predictor:
    def __init__(self, data, levels):
        self.data = data
        self.state = list(map(lambda level: dict(level=level, processed=set()), levels))

    def _predict_for_one_level(self, state_index):
        items = []
        for i in range(len(self.data)):
            if i not in self.state[state_index]['processed']:
                duplicates = [i] + self.recursive_search(i, state_index)
                items.append(duplicates)
        return dict(level=self.state[state_index]['level'], items=items)

    def recursive_search(self, row_index, state_index):
        self.state[state_index]['processed'].add(row_index)
        max_index = -1
        k = len(self.state[state_index]['level'])
        max_value = 0
        for (j, e) in enumerate(self.data[row_index]):
            if sum(e > self.state[state_index]['level']) == k and j not in self.state[state_index]['processed']:
                distance = np.linalg.norm(e)
                if distance > max_value:
                    max_value = distance
                    max_index = j
        if max_index != -1:
            return [max_index] + self.recursive_search(max_index, state_index)
        else:
            return []

    def predict_duplicates(self):
        results = list()

        for state_index in range(len(self.state)):
            r = self._predict_for_one_level(state_index)

            results.append(r)
        return results
