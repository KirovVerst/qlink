import os, json
from pandas import read_csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_SOURCES = {"mockaroo": os.path.join(BASE_DIR, "data", "mockaroo")}


def get_dataframe(source="mockaroo", kwargs=None):
    """
    
    :param source: 
    :param kwargs: {"init_data_size": int, "document_index":int}
    :return: pandas dataframe
    """
    if source == "mockaroo":
        filename = "data_{0}.csv".format(kwargs['document_index'])
        folder = os.path.join(DATA_SOURCES[source], "ready", str(kwargs['init_data_size']))
        path = os.path.join(folder, filename)
        df = read_csv(path)
        return df
    else:
        raise RuntimeError("Source \'{0}\' isn't supported. Sources: {1}".format(source, list(DATA_SOURCES.keys())))


def get_true_duplicates(source="mockaroo", kwargs=None):
    """
    
    :param source: 
    :param kwargs: 
    :return: 
    {
        'items': [[int,], [int,]]
    }
    """

    if source == "mockaroo":
        try:
            filename = "data_{0}.json".format(kwargs['document_index'])
            folder = os.path.join(DATA_SOURCES[source], "true_duplicates", str(kwargs['init_data_size']))
            path = os.path.join(folder, filename)
            with open(path, 'r') as f:
                truth = json.load(f)
            return truth
        except Exception as e:
            print(e)
    else:
        raise RuntimeError("Source \'{0}\' isn't supported. Sources: {1}".format(source, list(DATA_SOURCES.keys())))
