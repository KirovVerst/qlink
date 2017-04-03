import os, json
from pandas import read_csv, read_sql
from sqlalchemy.engine import create_engine
from conf import BASE_DIR

DATASET_TYPES = {"mockaroo": os.path.join(BASE_DIR, "data", "mockaroo")}


class Data(object):
    def __init__(self, dataset_type: str, kwargs=None):
        """
        
        :param dataset_type: 
        :param kwargs: dict
        if type == "mockaroo":
            kwargs = {
                    "init_data_size": int, 
                    "document_index": int
                }
        if type == "sql":
            kwargs = {
                "dialect": str. ``mysql``, ``sqlite``.
                "user": str. username, or empty string if dialect == "sqlite",
                "password": str. password, or empty string if dialect == "sqlite",
                "host": str. host, or empty string if dialect == "sqlite",
                "db_name": str. database name, or path to sqlite3 db if dialect == "sqlite",
                "query": str. raw sql query to database
            }
        """
        if dataset_type == "mockaroo":
            # dataset
            filename = "data_{0}.csv".format(kwargs['document_index'])
            folder = os.path.join(DATASET_TYPES[dataset_type], "ready", str(kwargs['init_data_size']))
            self.path_to_dataset = os.path.join(folder, filename)
            self.df = read_csv(self.path_to_dataset)

            # duplicates
            folder = os.path.join(DATASET_TYPES[dataset_type], "duplicates", str(kwargs['init_data_size']))
            filename = "data_{0}.json".format(kwargs['document_index'])
            self.path_to_true_duplicates = os.path.join(folder, filename)
            self.true_duplicates = {'items': []}
            with open(self.path_to_true_duplicates, 'r') as f:
                self.true_duplicates = json.load(f)
        elif dataset_type == "sql":
            """
            Url creating. 
            Url examples: 
                "dialect[+driver]://user:password@host/dbname[?key=value..]"
                "mysql://scott:tiger@hostname/dbname"
            """
            if kwargs["dialect"] == "mysql":
                kwargs["dialect"] += "+pymysql"
            url_dialect = kwargs["dialect"] + "://"
            url_credentials = ""
            if "user" in kwargs and "password" in kwargs and "host" in kwargs:
                url_credentials = kwargs["user"] + ":" + kwargs["password"] + "@" + kwargs["host"]

            url = url_dialect + url_credentials + "/" + kwargs["db_name"]
            self._engine = create_engine(url)
            self.df = read_sql(sql=kwargs['query'], con=self._engine)
            self.true_duplicates = []
        else:
            raise RuntimeError("Source \'{0}\' isn't supported.".format(dataset_type))
