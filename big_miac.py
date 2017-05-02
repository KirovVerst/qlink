import pandas as pd

from miac import Indexation, MIAC_BIG_DATA

if __name__ == "__main__":
    df = pd.read_csv(MIAC_BIG_DATA['full']['sample'], index_col='id')
    indexator = Indexation(df, index_field='last_name', index_output_path=MIAC_BIG_DATA['sample']['index'])
    indexator.create_index_dict()
