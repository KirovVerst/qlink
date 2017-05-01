import numpy as np
import os
import json

from collections import defaultdict
from pathos.multiprocessing import Pool

from modules.record_metrics import levenshtein_edit_distance
from modules.record_processing import get_strings, remove_double_letters


class EditDistanceMatrix(object):
    def __init__(self, df, str_column_names, date_column_names, index_path,
                 edit_distance_func=levenshtein_edit_distance,
                 normalize="sum", ):
        """
        
        :param df: 
        :param str_column_names: 
        :param edit_distance_func: 
        :param normalize: str. ``max``, ``sum``, ``total``, None
        """
        self.size = len(df)
        self.df = df
        self.str_column_names = str_column_names
        self.date_column_names = date_column_names
        self.x = defaultdict(list)
        self.func = edit_distance_func
        self.normalize = normalize
        self.max_dist = []
        self.k = len(self.str_column_names + self.date_column_names)
        # index dictionary
        with open(index_path, 'r') as fp:
            self.index_dict = json.load(fp)

    def process_part(self, row_indexes=None):
        """
        
        :param row_indexes: list of indexes
        :return: ([int, int, ...], np.array)
        """

        max_dist = [-1] * self.k
        if row_indexes is None:
            row_indexes = self.df.index.values.tolist()
        count = 0
        total_count = len(row_indexes)
        for i in row_indexes:
            available_row_ids = list(set(self.index_dict[self.df.loc[i]['last_name']]))
            try:
                available_row_ids.remove(i)
            except Exception:
                pass
            s1 = get_strings(self.df.loc[i], self.str_column_names, concat=self.k == 1)
            for field in self.date_column_names:
                s1.append(self.df.loc[i][field])

            for j in available_row_ids:

                s2 = get_strings(self.df.loc[j], self.str_column_names, concat=self.k == 1)
                for field in self.date_column_names:
                    s2.append(self.df.loc[j][field])

                distance = []

                for i1 in range(self.k):
                    current_dist = self.func(s1[i1], s2[i1])
                    if current_dist is None:
                        distance.append(current_dist)
                        continue
                    if self.normalize == "max":
                        max_d = max(len(s1[i1]), len(s2[i1]))
                        distance.append((max_d - current_dist) / max_d)
                    elif self.normalize == "sum":
                        max_d = len(s1[i1]) + len(s2[i1])
                        distance.append((max_d - current_dist) / max_d)
                    else:
                        distance.append(current_dist)

                    max_dist[i1] = max(max_dist[i1], current_dist)

                if None not in s1 and (j, distance) not in self.x[i]:
                    self.x[i].append((j, distance.copy()))

                if None not in s2 and (i, distance) not in self.x[j]:
                    self.x[j].append((i, distance.copy()))
            count += 1
            if count % 500 == 0:
                print(os.getpid(), ' : ', round(count / total_count, 3))
        return max_dist, self.x

    def get_ids(self, njobs):
        r = self.size % njobs
        indexes = self.df.index.values.tolist()
        if r > 0:
            matrix = np.reshape(indexes[:-r], (-1, njobs))
        else:
            matrix = np.reshape(indexes, (-1, njobs))
        matrix = matrix.transpose().tolist()
        if r > 0:
            for i in indexes[-r:]:
                matrix[0].append(i)

        return matrix

    def get(self, njobs=-1):
        """
            
            :param  njobs: number of threads. If njobs is -1, all threads will be used. 
                    njobs must be 1 if the method 'get' is calling from not main process 
                    else the following exception will be raised:
                    "AssertionError: daemonic processes are not allowed to have children"
            :return: 
            {
                values: numpy.ndarray. (self.size * self.size * self.k)
                max_dist: int. max distance that is contained in the matrix
            }
            """

        if njobs == 1:
            max_dist, x = self.process_part()
            self.x = x
            self.max_dist = max_dist
        else:
            if njobs == -1:
                njobs = os.cpu_count()
            indexes = self.get_ids(njobs)

            with Pool(njobs) as p:
                results = list(p.map(self.process_part, indexes))

            distances = np.array(list(map(lambda result: result[0], results)))
            distances = distances.transpose()

            matrixes = list(map(lambda result: result[1], results))
            for matrix in matrixes:
                for k, v in matrix.items():
                    self.x[k].extend(v)

            self.max_dist = list(map(lambda i: max(distances[i]), range(self.k)))

        return {
            'values': self.x,
            'max_dist': list(map(int, self.max_dist))
        }

    def _init_index(self, index_fields):
        if index_fields is None:
            index_fields = ['first_name']
        self.index_dict = dict()
        for field_name in index_fields:
            self.df[field_name] = self.df[field_name].apply(remove_double_letters)
            self.index_dict[field_name] = defaultdict(list)

        for row_id, row in self.df.iterrows():
            for field_name in index_fields:
                field_value = row[field_name]
                rows = self.df[self.df[field_name].str.contains(field_value)].index.values.tolist()

                self.index_dict[field_name][field_value] = list(set(rows + self.index_dict[field_name][field_value]))
