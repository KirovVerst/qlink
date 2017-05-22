import pandas as pd
import json
import os
from conf import MIAC_SMALL_DATA, MIAC_STR_FIELDS, MIAC_DATE_FIELDS, MIAC_SMALL_FOLDER
from modules.indexation import Indexation
from modules.matrix_calculation import MatrixCalculation
from modules.duplicate_searching import DuplicateSearching


def run(is_sample, size=None):
    dataframe = pd.read_csv(MIAC_SMALL_DATA['full']['data'], index_col='id')
    folder = MIAC_SMALL_DATA['full']

    if is_sample:
        if size is None:
            size = 1000
        dataframe = dataframe[:size]
        dataframe.to_csv(MIAC_SMALL_DATA['sample']['data'], index=True)
        folder = MIAC_SMALL_DATA['sample']

    dataframe.fillna(value='', inplace=True)

    mode = 'letters'

    indexator = Indexation(dataframe=dataframe,
                           index_field='last_name',
                           mode=mode,
                           index_output_path=folder['index-letters'])
    indexator.create_index_dict(njobs=-1)

    matrix_calculator = MatrixCalculation(dataframe=dataframe,
                                          index_path=folder['index-letters'],
                                          matrix_path=folder['matrix'],
                                          index_field='last_name',
                                          norm_matrix_path=folder['norm-matrix'],
                                          str_fields=MIAC_STR_FIELDS,
                                          date_fields=MIAC_DATE_FIELDS)
    matrix_calculator.create_matrix(njobs=-1)

    searcher = DuplicateSearching(dataframe=dataframe,
                                  norm_matrix_path=folder['norm-matrix'],
                                  duplicates_path=folder['duplicates'],
                                  mode='all')
    searcher.search_duplicates([[0.8, 0.8, 0.8, 0.9]])


def search(is_sample):
    folder = MIAC_SMALL_DATA['sample'] if is_sample else MIAC_SMALL_DATA['full']
    dataframe = pd.read_csv(folder['data'], index_col='id')
    searcher = DuplicateSearching(dataframe=dataframe,
                                  norm_matrix_path=folder['norm-matrix'],
                                  duplicates_path=folder['duplicates'],
                                  mode='all')
    searcher.search_duplicates([[0.8, 0.8, 0.8, 0.9]])


if __name__ == '__main__':
    # search(is_sample=False)
    with open(MIAC_SMALL_DATA['full']['duplicates'], 'r') as fp:
        duplicates = json.load(fp)
    duplicates = duplicates[0]['items']
    one = 0
    two = 0
    three = 0
    more = 0
    for cluster in duplicates:
        size = len(cluster.keys())
        if size > 3:
            more += 1
        elif size > 2:
            three += 1
        elif size > 1:
            two += 1
        elif size == 1:
            one += 1
    df = pd.read_csv(MIAC_SMALL_DATA['full']['data'])
    total = len(df)
    unique = total - two - 2 * three
    print('one : ', total - two * 2 - three * 3)
    print('two : ', two)
    print('three : ', three)
    print('more : ', more)
    print('total : ', total)
    print('unique : ', unique)

    """
    r = r'[a-zA-Z0-9.,*@]'
    df = pd.read_csv('/Users/Kirov/Science/RecordLinkage/data/miac/small/data-original.csv')
    temp = pd.concat([df[df['father_name'].str.contains('глы', na=False)],
                      df[df['father_name'].str.contains('кызы', na=False)],
                      df[df['father_name'].str.contains('кзы', na=False)]])
    print('father_name', " : ", len(temp))
    print(temp)
    """
