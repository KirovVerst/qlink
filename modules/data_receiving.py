import os, json
from pandas import read_csv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_TYPES = {"mockaroo": os.path.join(BASE_DIR, "data", "mockaroo")}


class Data(object):
    def __init__(self, type, kwargs=None):
        """
        
        :param type: 
        :param kwargs: {"init_data_size": int, "document_index":int}
        """
        if type == "mockaroo":
            # dataset
            filename = "data_{0}.csv".format(kwargs['document_index'])
            folder = os.path.join(DATA_TYPES[type], "ready", str(kwargs['init_data_size']))
            self.path_to_dataset = os.path.join(folder, filename)
            self.df = read_csv(self.path_to_dataset)

            # duplicates
            folder = os.path.join(DATA_TYPES[type], "true_duplicates", str(kwargs['init_data_size']))
            filename = "data_{0}.json".format(kwargs['document_index'])
            self.path_to_true_duplicates = os.path.join(folder, filename)
            self.true_duplicates = {'items': []}
            with open(self.path_to_true_duplicates, 'r') as f:
                self.true_duplicates = json.load(f)
        else:
            raise RuntimeError("Source \'{0}\' isn't supported.".format(type))
