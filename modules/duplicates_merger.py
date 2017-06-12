import json


class Merger:
    RANDOM_MODE = 0
    ENRICH_MODE = 1

    def __init__(self, dataframe, duplicates_path, result_path, merger_mode):
        self.dataframe = dataframe
        with open(duplicates_path, 'r') as fp:
            self.duplicates = json.load(fp)[0]['items']
        self.result_path = result_path
        self.merger_mode = merger_mode

    def enrich_records(self, cluster):
        """

        :param cluster:
        :return: int, row
        """
        cluster = sorted(cluster)
        main_record = self.dataframe.loc[cluster[0]]
        for field_name in self.dataframe.columns.values:
            if main_record[field_name] in [None, 'nan']:
                for other_id in cluster[1:]:
                    potential_value = self.dataframe.iloc[other_id][field_name]
                    if potential_value not in [None, 'nan']:
                        main_record[field_name] = potential_value
                        break
        return cluster[0], main_record

    def merge_duplicates(self):
        duplicate_ids = list(map(lambda d: list(map(int, d.keys())), self.duplicates))
        for cluster in duplicate_ids:
            main_id, main_record = self.enrich_records(cluster)
            self.dataframe.loc[main_id] = main_record
            cluster.remove(main_id)
            for other_id in cluster:
                self.dataframe.drop(other_id, inplace=True)

    def save_results(self):
        self.dataframe.to_csv(self.result_path, index=False)
