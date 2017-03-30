from math import ceil


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
        max_value = 0
        k = len(level)
        min_number_fields = k
        if k > 1:
            min_number_fields = ceil(k / 2)
        for (j, e) in enumerate(x[i]):
            if sum(e > level) == k and j not in processed and sum(e > max_value) > min_number_fields:
                max_value = e
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
