import pandas as pd
from modules.miac import MIAC_SMALL_DATA
from modules.miac import Indexation, MatrixCalculation, Normalization, DuplicateSearching


def new_sample(size):
    df = pd.read_csv(MIAC_SMALL_DATA['data'], index_col='id')
    df = df[:size]
    df.to_csv(MIAC_SMALL_DATA['sample']['data'], index=True)

    indexator = Indexation(df, 'last_name', MIAC_SMALL_DATA['sample']['index'])
    indexator.create_index_dict()

    matrix_calculator = MatrixCalculation(df, MIAC_SMALL_DATA['sample']['index'], MIAC_SMALL_DATA['sample']['matrix'])
    matrix_calculator.create_matrix()

    normalizator = Normalization(df, MIAC_SMALL_DATA['sample']['matrix'], MIAC_SMALL_DATA['sample']['norm-matrix'])
    normalizator.create_norm_matrix()

    searcher = DuplicateSearching(df, MIAC_SMALL_DATA['sample']['norm-matrix'], MIAC_SMALL_DATA['sample']['duplicates'])

    searcher.search_duplicates(0.85)


def search_sample():
    df = pd.read_csv(MIAC_SMALL_DATA['sample']['data'], index_col='id')
    searcher = DuplicateSearching(df, MIAC_SMALL_DATA['sample']['norm-matrix'], MIAC_SMALL_DATA['sample']['duplicates'])
    searcher.search_duplicates(0.85)


if __name__ == '__main__':
    new_sample(2000)
