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


def error_number(true, predict, N):
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

    while i1 < N or i2 < N:
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