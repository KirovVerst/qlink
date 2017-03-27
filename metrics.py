from preprocessing import remove_double_letters


def edit_distance(a, b):
    """Calculates the Levenshtein distance between a and b."""
    n, m = len(a), len(b)
    if n > m:
        a, b = b, a
        n, m = m, n

    current_row = range(n + 1)
    for i in range(1, m + 1):
        previous_row, current_row = current_row, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete, change = previous_row[j] + 1, current_row[j - 1] + 1, previous_row[j - 1]
            if a[j - 1] != b[i - 1]:
                change += 1
            current_row[j] = min(add, delete, change)

    return current_row[n]


def error_number(true, predict):
    """
    There is the set of objects. "true" is the set partition. "predict" is another one.
    Function calculates the number of differences between this partitions.
    Example 1: 
        true = [[1, 2]]
        predict = [[1], [2]]
        errors = 1 
    :param true: list of lists 
    :param predict: list of lists
    :param N: total number of unique objects
    :return: num_error, errors
    """

    result = 0
    i1 = 0
    n1 = len(true)
    i2 = 0
    n2 = len(predict)
    errors = []

    while i1 < n1 or i2 < n2:
        d = 0
        if i1 < n1:
            s1 = set(true[i1])
            x = true[i1][0]
        else:
            if i2 < n2:
                rest = predict[i2:]
                result += sum([len(arr) for arr in rest])
            break

        if i2 < n2:
            s2 = set(predict[i2])
            y = predict[i2][0]
        else:
            if i1 < n1:
                rest = true[i1:]
                result += sum([len(arr) for arr in rest])
            break

        if len(s1.intersection(s2)):
            d += len(s1.symmetric_difference(s2))
            if d > 0:
                errors.append(dict(true=s1, pred=s2))
            i1 += 1
            i2 += 1

        else:
            if y < x:
                d += len(s2)
                i2 += 1

            else:
                d += len(s1)
                i1 += 1
        result += d

    return result / 2, errors


def edit_distance_matrix(data, columns):
    dataset_size = len(data)
    x = [[0] * dataset_size for _ in range(dataset_size)]

    """
    Levenshtein distance calculation
    """
    max_dist = -1
    for i in range(dataset_size):
        s1 = ""
        for column_name in columns:
            s1 += remove_double_letters(data.iloc[i][column_name])

        for j in range(i + 1, dataset_size):
            s2 = ""
            for column_name in columns:
                s2 += remove_double_letters(data.iloc[j][column_name])

            d = edit_distance(s1, s2)
            if d > max_dist:
                max_dist = d
            x[i][j] = d
            x[j][i] = d

    # print("Levenshtein distances have been calculated")
    """
    Levenshtein distance normalization
    """
    if max_dist == 0:
        return x
    for i in range(dataset_size):
        x[i] = list(map(lambda y: (max_dist - y) / max_dist, x[i]))
        x[i][i] = 0

    # print("Normalization has been done")
    return x
