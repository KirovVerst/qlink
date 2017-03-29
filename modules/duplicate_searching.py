def predict_duplicates(data, level):
    """

    :param data: [[float], [float] ]
    :param level: float 
    :return: 
    {
        'items': [[int, ], [int, ]]
    }
    """

    def rec(i, x):
        processed.add(i)
        max_index = -1
        max_value = 0
        for (j, e) in enumerate(x[i]):
            if e > level and j not in processed and e > max_value:
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


