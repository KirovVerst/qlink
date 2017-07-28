import os
import re

from qlink.dataset_receiving import Data

from examples.conf import BASE_DIR

MIAC_PATH = os.path.join(BASE_DIR, 'data', 'miac')
DATASET_XLS_PATH = os.path.join(MIAC_PATH, 'data2.xlsx')
DATASET_CSV_PATH = os.path.join(MIAC_PATH, 'data2.csv')
ALPHA_REGEX = re.compile('^[а-яА-ЯёЁ]')
NUM_REGEX = re.compile('^[0-9]')


class DataPreprocessing:
    str_fields = ['first_name', 'last_name', 'father_name']
    date_fields = ['birthday']
    fields = str_fields + date_fields

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def to_lower(self):
        for column in self.str_fields:
            self.dataframe[column] = self.dataframe[column].apply(str).apply(str.lower)

    @staticmethod
    def lower(s):
        try:
            return s.lower()
        except:
            return ''

    @staticmethod
    def clean_string(s):

        if type(s) is str:
            return NUM_REGEX.sub('', s)
        else:
            return ''

    def clean(self):
        for column in self.str_fields:
            self.dataframe[column] = self.dataframe[column].apply(self.clean_string)

    def rename_columns(self):
        self.dataframe.rename(
            columns={
                'Фамилия': 'last_name', 'Имя': 'first_name', 'Отчество': 'father_name', 'Дата рождения': 'birthday'
            },
            inplace=True
        )

    def to_csv(self, path):
        self.dataframe.to_csv(path, index=False)

    @staticmethod
    def get_birthday(s):
        if type(s) is str:
            return s.split()[0]
        else:
            return s

    def split_birthday(self):
        self.dataframe['birthday'] = self.dataframe['birthday'].apply(self.get_birthday)

    def remove_null_rows(self):
        self.dataframe.dropna(axis='index', how='all', inplace=True)


MIAC_TEST_FOLDER = os.path.join(MIAC_PATH, 'test')
MIAC_TEST_INDEX_FOLDER = os.path.join(MIAC_TEST_FOLDER, 'index')
MIAC_TEST_MATRIX_FOLDER = os.path.join(MIAC_TEST_FOLDER, 'matrix')
MIAC_TEST_NORM_MATRIX_FOLDER = os.path.join(MIAC_TEST_FOLDER, 'norm-matrix')

if __name__ == '__main__':
    data = Data(dataset_type="miac_test", kwargs=dict(document_index=0))
    data.df.drop(['original_id'], axis=1, inplace=True)
    data.df.to_csv('data.csv', index=True)
    """
    index_path = os.path.join(MIAC_TEST_INDEX_FOLDER, 'data_{}.json'.format(0))
    indexator = Indexation(dataframe=data.df,
                           index_field='last_name',
                           index_output_path=index_path,
                           mode=Indexation.MODE_LETTERS,
                           level=0.85)
    indexator.create_index_dict(njobs=1)

    matrix_path = os.path.join(MIAC_TEST_MATRIX_FOLDER, 'data_{}.json'.format(0))
    norm_matrix_path = os.path.join(MIAC_TEST_NORM_MATRIX_FOLDER, 'data_{}.json'.format(0))
    calculator = MatrixCalculation(dataframe=data.df, date_fields=MIAC_DATE_FIELDS, str_fields=MIAC_STR_FIELDS,
                                   index_field='last_name', index_path=index_path,
                                   norm_matrix_path=norm_matrix_path, matrix_path=matrix_path)
    calculator.create_matrix()
    """
