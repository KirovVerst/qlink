import pandas as pd
import os
import re
import datetime
import json
from conf import BASE_DIR

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


if __name__ == '__main__':
    start = datetime.datetime.now()
    print("Start: {}".format(start))
    df = pd.read_csv(DATASET_CSV_PATH)
    processor = DataPreprocessing(df)
    processor.clean()
    processor.to_csv(path=os.path.join(MIAC_PATH, 'data-3.csv'))
