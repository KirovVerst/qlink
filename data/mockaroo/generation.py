import pandas as pd
import numpy as np
import os, json
from multiprocessing import Pool
from collections import defaultdict
from data.mockaroo.mistake_generation import duplicate_rows, add_mistakes

columns = dict(first_name=0.3, last_name=0.3, father=0.3)
NUMBER_OF_DATASETS = 6
NUMBER_OF_RAW_DOCUMENTS = 12

for N in [2000]:
    data_folder_path = 'ready/{0}'.format(N)
    if not os.path.exists(data_folder_path):
        os.mkdir(data_folder_path)

    duplicates_folder_path = 'true_duplicates/{0}'.format(N)
    if not os.path.exists(duplicates_folder_path):
        os.mkdir(duplicates_folder_path)


    def func(dataset_id, raw_doc_id):
        doc_id = raw_doc_id
        s = 'original/data_{0}.csv'.format(doc_id)
        full_data = pd.read_csv(s)
        while len(full_data) < N:
            doc_id += 1
            s = 'original/data_{0}.csv'.format(doc_id)
            part_data = pd.read_csv(s)
            full_data = pd.concat([full_data, part_data])

        data = full_data.sample(N, random_state=dataset_id)
        data["original_id"] = np.arange(0, N)

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
        return doc_id - raw_doc_id + 1


    used_raw_docs_number = 0
    for i in range(NUMBER_OF_DATASETS):
        used_raw_docs_number += func(i, used_raw_docs_number)
