import pandas as pd
import os
from duplicate_searching import DuplicateSearching
from conf import MIAC_BIG_DATA, MIAC_BIG_FOLDER, MIAC_STR_FIELDS


def generate_sample():
    df_sample = pd.read_csv(MIAC_BIG_DATA['sample']['data'], index_col='id')
    df = pd.read_csv(MIAC_BIG_DATA['full']['data'], index_col='id')
    df = df[pd.isnull(df['last_name'])]
    df_sample = pd.concat([df_sample, df])
    df_sample.to_csv(MIAC_BIG_DATA['sample']['data'], index=True)


def get_sample():
    df = pd.read_csv(MIAC_BIG_DATA['sample']['data'], index_col='id')
    df = df.astype('str')
    return df


if __name__ == "__main__":
    df = pd.read_csv(os.path.join(MIAC_BIG_DATA['full']['data']), index_col='id')
    df = df[df['last_name'].str[0] == 'а']
    df.sort_values(by=['last_name', 'first_name'], inplace=True)
    df = df[:1000]
    df.to_csv('new-dataset.csv', index=True)
