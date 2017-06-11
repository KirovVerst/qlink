import pandas as pd
import os

from modules.indexation import Indexation
from modules.matrix_calculation import MatrixCalculation
from modules.duplicate_searching import DuplicateSearching
from conf import BASE_DIR, MIAC_STR_FIELDS, MIAC_DATE_FIELDS

DATA_FOLDER = os.path.join(BASE_DIR, "data", "miac", "test")

DOCUMENT_INDEX = 2

INDEX_PATH = 'index_{}.json'.format(DOCUMENT_INDEX)
NORM_MATRIX_PATH = 'norm_matrix_{}.json'.format(DOCUMENT_INDEX)
MATRIX_PATH = 'matrix_{}.json'.format(DOCUMENT_INDEX)
DUPLICATES_PATH = 'duplicates_{}.json'.format(DOCUMENT_INDEX)

if __name__ == '__main__':
    dataframe = pd.read_csv(os.path.join(DATA_FOLDER, 'ready', 'data_{}.csv'.format(DOCUMENT_INDEX)))
    indexator = Indexation(dataframe=dataframe, index_field='last_name',
                           index_output_path=INDEX_PATH,
                           mode=Indexation.MODE_LETTERS, level=0.8)

    indexator.create_index_dict(njobs=-1)

    calculator = MatrixCalculation(dataframe=dataframe, str_fields=MIAC_STR_FIELDS, date_fields=MIAC_DATE_FIELDS,
                                   index_path=INDEX_PATH, index_field='last_name',
                                   norm_matrix_path=NORM_MATRIX_PATH,
                                   matrix_path=MATRIX_PATH)

    calculator.create_matrix(njobs=-1)

    searcher = DuplicateSearching(dataframe=dataframe, norm_matrix_path=NORM_MATRIX_PATH,
                                  duplicates_path=DUPLICATES_PATH, mode='all')

    searcher.search_duplicates(levels=[[0.8, 0.8, 0.8, 0.9]])
