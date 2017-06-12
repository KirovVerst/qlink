import pandas as pd
import os
import datetime

from modules.indexation import Indexation
from modules.matrix_calculation import MatrixCalculation
from modules.duplicate_searching import Searcher
from modules.duplicates_merger import Merger
from conf import BASE_DIR, MIAC_STR_FIELDS, MIAC_DATE_FIELDS

DATA_FOLDER = os.path.join(BASE_DIR, "data", "miac", "test")

DOCUMENT_INDEX = 2

READY_DATA_PATH = os.path.join(DATA_FOLDER, 'ready', 'data_{}.csv'.format(DOCUMENT_INDEX))
INDEX_PATH = os.path.join(DATA_FOLDER, 'index', 'index_{}.json'.format(DOCUMENT_INDEX))
NORM_MATRIX_PATH = os.path.join(DATA_FOLDER, 'norm_matrix', 'norm_matrix_{}.json'.format(DOCUMENT_INDEX))
MATRIX_PATH = os.path.join(DATA_FOLDER, 'matrix', 'matrix_{}.json'.format(DOCUMENT_INDEX))
DUPLICATES_PATH = os.path.join(DATA_FOLDER, 'results', 'duplicates_{}.json'.format(DOCUMENT_INDEX))
RESULT_PATH = os.path.join(DATA_FOLDER, 'results', 'dataset_{}.csv'.format(DOCUMENT_INDEX))
INDEX_FIELD = 'last_name'

if __name__ == '__main__':
    s = datetime.datetime.now()
    dataframe = pd.read_csv(READY_DATA_PATH)
    indexator = Indexation(dataframe=dataframe,
                           index_field=INDEX_FIELD,
                           index_output_path=INDEX_PATH,
                           mode=Indexation.MODE_LETTERS,
                           level=0.5)

    # indexator.create_index_dict(njobs=-1)

    calculator = MatrixCalculation(dataframe=dataframe,
                                   str_fields=MIAC_STR_FIELDS,
                                   date_fields=MIAC_DATE_FIELDS,
                                   index_path=INDEX_PATH,
                                   index_field=INDEX_FIELD,
                                   norm_matrix_path=NORM_MATRIX_PATH,
                                   matrix_path=MATRIX_PATH)

    # calculator.create_matrix(njobs=-1)

    searcher = Searcher(dataframe=dataframe,
                        norm_matrix_path=NORM_MATRIX_PATH,
                        duplicates_path=DUPLICATES_PATH,
                        mode='all')

    # searcher.search_duplicates(levels=[[0.8, 0.8, 0.8, 0.9]])
    merger = Merger(dataframe=dataframe, duplicates_path=DUPLICATES_PATH,
                    result_path=RESULT_PATH, merger_mode=Merger.ENRICH_MODE)

    merger.merge_duplicates()
    merger.save_results()
