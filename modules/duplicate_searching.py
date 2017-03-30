from math import ceil
import numpy as np

def predict_duplicates(data, level):
    """

    :param data: [
                    [[float, ...], [float, ...]],
                    [[float, ...], [float, ...]],
                    [[float, ...], [float, ...]]
                ]
    :param level: [float, float] 
    :return: 
    {
        'items': [[int, ], [int, ]]
    }
    """

    def rec(i, x):
        processed.add(i)
        max_index = -1
        k = len(level)
        max_value = 0
        for (j, e) in enumerate(x[i]):
            if sum(e > level) == k and j not in processed:
                distance = np.linalg.norm(e)
                if distance > max_value:
                    max_value = distance
                    max_index = j
        if max_index != -1:
            return [max_index] + rec(max_index, x)
        else:
            return []

    results = list()
    processed = set()
    for i in range(len(data)):
        if i not in processed:
            duplicates = [i] + rec(i, data)
            results.append(duplicates)
    return dict(items=results)
