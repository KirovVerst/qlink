import os
import pandas as pd
import json
import numpy as np
import datetime
import _pickle as pickle
from collections import defaultdict
from pathos.multiprocessing import Pool

from conf import BASE_DIR
from modules.dataset_processing import EditDistanceMatrix
from duplicate_searching import Predictor

from modules.miac import MIAC_SMALL_DATA
from modules.miac import Indexation, MatrixCalculation, Normalization

FOLDER_PATH = os.path.join(BASE_DIR, 'data', 'miac', 'small')
DATAFRAME_PATH = os.path.join(FOLDER_PATH, 'data.csv')
INDEX_PATH = os.path.join(FOLDER_PATH, 'index.json')
MATRIX_PATH = os.path.join(FOLDER_PATH, 'matrix.pkl')
MATRIX_PATH_JSON = os.path.join(FOLDER_PATH, 'matrix.json')

STR_FIELDS = ['first_name', 'last_name', 'father_name']
DATE_FIELDS = ['birthday']


def row_to_str(row):
    r = ''
    for field in STR_FIELDS + DATE_FIELDS:
        r += str(row[field]) + ' '
    return r


def search_duplicates(norm_matrix_path, duplicates_path):
    with open(norm_matrix_path, 'r') as fp:
        matrix = json.load(fp)
    predictor = Predictor(data=matrix, levels=[[1] * 4],
                          list2float='sum', comparator='and', save_extra_data=False)
    duplicates = predictor.predict_duplicates(njobs=1)
    new_duplicates_items = []
    for duplicates_one_level in duplicates:
        items = duplicates_one_level['items']
        for ids in items:
            ids = list(set(map(lambda x: int(x), ids)))
            info = dict()
            for index in ids:
                info[index] = row_to_str(df.loc[index])

            new_duplicates_items.append(info)
    duplicates[0]['items'] = new_duplicates_items
    with open(duplicates_path, 'w') as fp:
        json.dump(duplicates, fp, ensure_ascii=False)


if __name__ == '__main__':
    df = pd.read_csv(MIAC_SMALL_DATA['sample']['data'], index_col='id')
    normalizator = Normalization(df, matrix_path=MIAC_SMALL_DATA['sample']['matrix'],
                                 norm_matrix_output_path=MIAC_SMALL_DATA['sample']['norm-matrix'])

    normalizator.create_norm_matrix()
