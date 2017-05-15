import pandas as pd
import json
from conf import MIAC_SMALL_DATA, MIAC_STR_FIELDS, MIAC_DATE_FIELDS
from modules.indexation import Indexation
from modules.matrix_calculation import MatrixCalculation
from duplicate_searching import DuplicateSearching


def run(is_sample, size=None):
    dataframe = pd.read_csv(MIAC_SMALL_DATA['full']['data'], index_col='id')
    folder = MIAC_SMALL_DATA['full']

    if is_sample:
        if size is None:
            size = 1000
        dataframe = dataframe[:size]
        dataframe.to_csv(MIAC_SMALL_DATA['sample']['data'], index=True)
        folder = MIAC_SMALL_DATA['sample']

    matrix_calculator = MatrixCalculation(dataframe=dataframe,
                                          index_path=folder['index'],
                                          matrix_path=folder['matrix'],
                                          index_field='last_name',
                                          norm_matrix_path=folder['norm-matrix'],
                                          str_fields=MIAC_STR_FIELDS,
                                          date_fields=MIAC_DATE_FIELDS)
    matrix_calculator.create_matrix()

    searcher = DuplicateSearching(dataframe=dataframe,
                                  norm_matrix_path=folder['norm-matrix'],
                                  duplicates_path=folder['duplicates'],
                                  mode='all')
    searcher.search_duplicates([0.87, 0.87, 0.87, 0.9])


def search(is_sample):
    folder = MIAC_SMALL_DATA['sample'] if is_sample else MIAC_SMALL_DATA['full']
    dataframe = pd.read_csv(folder['data'], index_col='id')
    searcher = DuplicateSearching(dataframe=dataframe,
                                  norm_matrix_path=folder['norm-matrix'],
                                  duplicates_path=folder['duplicates'],
                                  mode='all')
    searcher.search_duplicates([0.83, 0.83, 0.85, 0.92])


if __name__ == '__main__':
    df = pd.read_csv(MIAC_SMALL_DATA['sample']['data'], index_col='id')
    indexator = Indexation(df, 'last_name', MIAC_SMALL_DATA['sample']['index-letters'], mode='letters')
    indexator.create_index_dict()
