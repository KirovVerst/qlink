import pandas as pd
import os, json
from multiprocessing import Pool
from collections import defaultdict
from data.mockaroo.mistake_generation import duplicate_rows, add_mistakes

columns = dict(first_name=0.3, last_name=0.3, father=0.3)
DATASET_NUMBER_PER_DOCUMENT = 1
RAW_DOCUMENT_NUMBER = 12

for N in [100, 200, 1000]:
    data_folder_path = 'ready/{0}'.format(N)
    if not os.path.exists(data_folder_path):
        os.mkdir(data_folder_path)

    duplicates_folder_path = 'true_duplicates/{0}'.format(N)
    if not os.path.exists(duplicates_folder_path):
        os.mkdir(duplicates_folder_path)


    def func(dataset_id):
        raw_doc_id = dataset_id // DATASET_NUMBER_PER_DOCUMENT
        s = 'original/data_{0}.csv'.format(raw_doc_id)
        full_data = pd.read_csv(s)
        data = full_data.sample(N, random_state=dataset_id)
        extended_data = duplicate_rows(data, rng=2, frac=0.3)

        changed_data = add_mistakes(extended_data, columns)

        full_path = os.path.join(data_folder_path, 'data_{0}.csv'.format(dataset_id))
        changed_data.to_csv(full_path, index=False)

        """
        Duplicates search
        """
        changed_data = changed_data.reset_index(drop=True)
        truth = defaultdict(list)
        for j, row in changed_data.iterrows():
            truth[row['original_id']].append(int(j))

        full_path = os.path.join(duplicates_folder_path, 'data_{0}.json'.format(dataset_id))
        with open(full_path, 'w') as fp:
            json.dump(dict(items=list(truth.values())), fp)


    with Pool(1) as p:
        p.map(func, list(range(RAW_DOCUMENT_NUMBER * DATASET_NUMBER_PER_DOCUMENT)))
