import json
import os
from collections import defaultdict

import numpy as np
import pandas as pd

from mistake_generation import duplicate_rows, add_mistakes


class DatasetGenerator:
    def __init__(self, raw_data_folder: str, destinations: list, dataset_conf: dict):
        """
        
        :param raw_data_folder: Where from to read raw data.
        :param destinations: Configurations where to write datasets. 
        [{type="csv", dataset_folder="str", duplicate_folder="str"}]
        :param dataset_conf: 
        {
            "number": int,
            "init_size": int,
            "mistakes": dict(first_name=0.3, last_name=0.3, father=0.3),
            "duplicates":{
                "range": 2,
                "frac": 0.3
            }
            
        }
        """
        paths = list(map(lambda path: os.path.join(raw_data_folder, path), os.listdir(raw_data_folder)))
        self.raw_file_paths = set(filter(os.path.isfile, paths))

        self.destinations = destinations
        self.dataset_conf = dataset_conf
        self._used_raw_file_paths = set()

    def write_datasets(self):
        for i in range(self.dataset_conf["number"]):
            self._write_dataset(dataset_id=i)

    def _write_to_csv(self, dataset_folder, duplicate_folder, dataset_id):
        """
        
        :param dataset_folder: 
        :param duplicate_folder: 
        :param dataset_id: 
        :return: 
        """

        dataset_path = os.path.join(dataset_folder, 'data_{0}.csv'.format(dataset_id))
        self._df.to_csv(dataset_path, index=False)

        duplicate_path = os.path.join(duplicate_folder, 'data_{0}.json'.format(dataset_id))

        with open(duplicate_path, 'w') as fp:
            json.dump(dict(items=list(self._true_duplicates.values())), fp)

    @property
    def _true_duplicates(self):
        """
        
        :return: dict(1 = [2,3,4], 2=[6,8,9,67,78], ...)
        """
        self._df.reset_index(drop=True, inplace=True)
        true_duplicates = defaultdict(list)
        for j, row in self._df.iterrows():
            true_duplicates[row['original_id']].append(int(j))
        return true_duplicates

    def _write_dataset(self, dataset_id):
        full_data = pd.DataFrame()

        while len(full_data) < self.dataset_conf["init_size"]:
            if len(self.raw_file_paths) == 0:
                self.raw_file_paths = set(self._used_raw_file_paths)
                self._used_raw_file_paths = set()

            raw_file_path = self.raw_file_paths.pop()
            part_data = pd.read_csv(raw_file_path)
            full_data = pd.concat([full_data, part_data])
            self._used_raw_file_paths.add(raw_file_path)

        data = full_data.sample(self.dataset_conf["init_size"], random_state=dataset_id)
        data["original_id"] = np.arange(0, self.dataset_conf["init_size"])

        extended_data = duplicate_rows(data=data,
                                       rng=self.dataset_conf["duplicates"]["range"],
                                       frac=self.dataset_conf["duplicates"]["frac"])

        changed_data = add_mistakes(extended_data, self.dataset_conf['mistakes'])
        self._df = changed_data
        for destination in self.destinations:
            if destination["type"] == "csv":
                self._write_to_csv(dataset_folder=destination["dataset_folder"],
                                   duplicate_folder=destination["duplicate_folder"],
                                   dataset_id=dataset_id)


if __name__ == "__main__":
    for size, number in [(100, 12)]:
        folder = os.path.join(os.path.pardir, "data", "mockaroo")
        dataset_folder_path = os.path.join(folder, 'ready', str(size))
        if not os.path.exists(dataset_folder_path):
            os.makedirs(dataset_folder_path)

        duplicate_folder_path = os.path.join(folder, 'duplicates', str(size))
        if not os.path.exists(duplicate_folder_path):
            os.makedirs(duplicate_folder_path, exist_ok=True)

        configuration = {
            "raw_data_folder": os.path.join(folder, "original"),
            "destinations": [
                {
                    "type": "csv",
                    "dataset_folder": os.path.join(dataset_folder_path),
                    "duplicate_folder": os.path.join(duplicate_folder_path)
                }
            ],
            "dataset_conf": {
                "number": number,
                "init_size": size,
                "mistakes": dict(first_name=0.3, last_name=0.3, father=0.3),
                "duplicates": {
                    "range": 2,
                    "frac": 0.3
                }
            }
        }
        generator = DatasetGenerator(**configuration)
        generator.write_datasets()

        print("Initial size: {0}\tNumber: {1}".format(size, number))
