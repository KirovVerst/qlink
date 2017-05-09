def get_differences(true, predict):
    """
    Function calculates the differences between two true and predicted partitions.
    Example 1: 
        true = [[1, 2]]
        predict = [[1], [2]]
        errors = 1 
    :param true: list of lists 
    :param predict: list of lists
    :param N: total number of unique objects
    :return: 
    {
        'items': [{true: [int], 'predict': [int]}],
        'number_of_errors': int
    }
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
            y = int(predict[i2][0])
        else:
            if i1 < n1:
                rest = true[i1:]
                result += sum([len(arr) for arr in rest])
            break

        if len(s1.intersection(s2)):
            d += len(s1.symmetric_difference(s2))
            if d > 0:
                errors.append(dict(true=s1, predict=s2))
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

    return {
        'items': errors,
        'number_of_errors': result / 2
    }


def get_accuracy(n_errors, n_records, n_digits=3):
    return round((n_records - n_errors) / n_records, n_digits)
