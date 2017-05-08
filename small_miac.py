import pandas as pd
import json
from modules.miac import MIAC_SMALL_DATA
from modules.miac import Indexation, MatrixCalculation, Normalization, DuplicateSearching


def run(is_sample, size=None):
    df = pd.read_csv(MIAC_SMALL_DATA['full']['data'], index_col='id')
    folder = MIAC_SMALL_DATA['full']

    if is_sample:
        df = df[:size]
        df.to_csv(MIAC_SMALL_DATA['sample']['data'], index=True)
        folder = MIAC_SMALL_DATA['sample']
    """
    indexator = Indexation(df, 'last_name', folder['index'])
    indexator.create_index_dict()
    """

    matrix_calculator = MatrixCalculation(df, folder['index'], folder['matrix'], index_field='last_name')
    matrix_calculator.create_matrix(norm_matrix_path=folder['norm-matrix'])

    searcher = DuplicateSearching(df, folder['norm-matrix'], folder['duplicates'], mode='all')
    searcher.search_duplicates([0.87, 0.87, 0.87, 0.9])


def search(is_sample):
    folder = MIAC_SMALL_DATA['sample'] if is_sample else MIAC_SMALL_DATA['full']
    df = pd.read_csv(folder['data'], index_col='id')
    searcher = DuplicateSearching(df, folder['norm-matrix'], folder['duplicates'], mode='all')
    searcher.search_duplicates([0.83, 0.83, 0.85, 0.92])


if __name__ == '__main__':
    with open(MIAC_SMALL_DATA['full']['duplicates']) as f:
        duplicates = json.load(f)

    duplicates = duplicates[0]['items']
    count = 0
    for cluster in duplicates:
        count += len(cluster.items())
    print(len(pd.read_csv(MIAC_SMALL_DATA['full']['data'])) - count + len(duplicates))
