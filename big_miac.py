import pandas as pd

from miac import Indexation, MIAC_BIG_DATA


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
    indexator = Indexation(df, index_field='last_name', index_output_path=MIAC_BIG_DATA['sample']['index'])
    indexator.create_index_dict()
