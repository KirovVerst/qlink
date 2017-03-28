from modules.record_metrics import levenshtein_edit_distance
from modules.record_processing import remove_double_letters


def edit_distance_matrix(df, column_names, edit_distance=levenshtein_edit_distance, normalize=True):
    """

    :param df: pandas dataframe
    :param column_names: list(str). list of column names in df
    :param edit_distance: function that calculates the edit distance between two strings, must be called with two str args.
    :param normalize: bool.
    :return: 
    {
        values: list of list of int. [[12, 34], [34, 45]]. shape = N*N, where N is len(df).
        max_dist: int. max distance that is contained in the matrix
    }
    """
    df_size = len(df)
    x = [[0] * df_size for _ in range(df_size)]

    """
    Levenshtein distance calculation
    """
    max_dist = -1
    for i in range(df_size):
        s1 = ""
        for column_name in column_names:
            s1 += remove_double_letters(df.iloc[i][column_name])

        for j in range(i + 1, df_size):
            s2 = ""
            for column_name in column_names:
                s2 += remove_double_letters(df.iloc[j][column_name])

            d = edit_distance(s1, s2)
            if d > max_dist:
                max_dist = d
            x[i][j] = d
            x[j][i] = d

    """
    Levenshtein distance normalization
    """
    if normalize:
        if max_dist != 0:
            for i in range(df_size):
                x[i] = list(map(lambda y: (max_dist - y) / max_dist, x[i]))
                x[i][i] = 0
        else:
            x = [[1] * df_size for _ in range(df_size)]
            for i in range(df_size):
                x[i][i] = 0
    return {
        'values': x,
        'max_dist': max_dist
    }


