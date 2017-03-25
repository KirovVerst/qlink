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


def error_number(list1, list2, N):
    """
    There is the set of objects. list1 is the set partition. list2 is another one.
    Function calculates the number of differences between this partitions.
    Example 1: 
        list1 = [[1, 2]]
        list2 = [[1], [2]]
        errors = 1 
    :param list1: list of lists 
    :param list2: list of lists
    :param N: total number of unique objects
    :return: 
    """

    result = 0
    i1 = 0
    n1 = len(list1)
    i2 = 0
    n2 = len(list2)

    while i1 < N or i2 < N:
        d = 0
        if i1 < n1:
            s1 = set(list1[i1])
            x = list1[i1][0]
        else:
            if i2 < n2:
                rest = list2[i2:]
                # print("i2 < n2")
                d += sum([len(arr) for arr in rest])
            break

        if i2 < n2:
            s2 = set(list2[i2])
            y = list2[i2][0]
        else:
            if i1 < n1:
                rest = list1[i1:]
                # print("i1 < n1")
                d += sum([len(arr) for arr in rest])
            break

        if len(s1.intersection(s2)):
            d += len(s1.symmetric_difference(s2))
            # if d > 0:
            #   print(list1[i1])
            #   print(list2[i2])
            i1 += 1
            i2 += 1

        else:
            if y < x:
                # print([])
                # print(list2[i2])
                d += len(s2)
                i2 += 1

            else:
                # print(list1[i1])
                # print([])
                d += len(s1)
                i1 += 1
                # if d > 0:
                # print(d)
                # print('\n')
        result += d

    return result // 2
