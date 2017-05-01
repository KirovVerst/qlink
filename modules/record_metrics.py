def levenshtein_edit_distance(a, b, normal=None):
    """Calculates the Levenshtein distance between a and b."""
    if not isinstance(a, str) or not isinstance(b, str):
        return None
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

    res = current_row[n]

    if normal is None:
        return res
    elif normal == "sum":
        sum_len = len(a) + len(b)
        return (sum_len - res) / sum_len
    elif normal == "max":
        max_len = max(len(a), len(b))
        return (max_len - res) / max_len
    else:
        raise RuntimeError("{} normalization isn't supported.".format(normal))
