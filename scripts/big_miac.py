import pandas as pd
from duplicate_searching import DuplicateSearching
from conf import MIAC_BIG_DATA


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
    df = get_sample()
    searcher = DuplicateSearching(dataframe=df, norm_matrix_path=MIAC_BIG_DATA['sample']['norm-matrix'],
                                  duplicates_path=MIAC_BIG_DATA['sample']['duplicates'], mode='all')
    searcher.search_duplicates(level=[0.79, 0.79, 0.79, 0.89])
