import numpy as np


def rearrange_list(values, n):
    r = len(values) % n
    if r > 0:
        rearranged_values = np.reshape(values[:-r], (-1, n))
    else:
        rearranged_values = np.reshape(values, (-1, n))
    rearranged_values = rearranged_values.transpose()
    rearranged_values = rearranged_values.tolist()
    if r > 0:
        if not isinstance(values, list):
            values = values.tolist()
        rearranged_values[0] += values[-r:]
    return rearranged_values


def merge_dicts(dicts):
    result_dict = dict()
    for one_dict in dicts:
        result_dict.update(one_dict)
    return result_dict
