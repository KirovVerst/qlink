import pandas as pd
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

    matrix_calculator = MatrixCalculation(df, folder['index'], folder['matrix'])
    matrix_calculator.create_matrix(norm_matrix_path=folder['norm-matrix'])

    searcher = DuplicateSearching(df, folder['norm-matrix'], folder['duplicates'])
    searcher.search_duplicates(0.9)


def search_sample():
    df = pd.read_csv(MIAC_SMALL_DATA['sample']['data'], index_col='id')
    searcher = DuplicateSearching(df, MIAC_SMALL_DATA['sample']['norm-matrix'], MIAC_SMALL_DATA['sample']['duplicates'])
    searcher.search_duplicates(0.9)


if __name__ == '__main__':
    run(is_sample=False)
