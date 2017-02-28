import pandas as pd

data = pd.read_csv('original/data_1.csv')


def duplicate_rows(data, frac=0.0):
    extra = data.sample(frac=frac)
    return pd.concat([data, extra])


"""
ex_example = duplicate_rows(data, frac=0.1)

ex_example.to_csv('extended/data_1.csv', index=False)
"""
