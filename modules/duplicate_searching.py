import numpy as np
import os
from pathos.multiprocessing import Pool


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

    def predict_duplicates(self, njobs=-1):
        if njobs <= -1 or njobs > os.cpu_count():
            njobs = os.cpu_count()
        with Pool(njobs) as p:
            results = list(p.map(self._predict_for_one_level, range(len(self.state))))

        return results
