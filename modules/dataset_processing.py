import numpy as np
import os

from collections import defaultdict
from pathos.multiprocessing import Pool

from modules.record_metrics import levenshtein_edit_distance
from modules.record_processing import get_strings, remove_double_letters


class EditDistanceMatrix(object):
    def __init__(self, df, column_names,
                 edit_distance_func=levenshtein_edit_distance,
                 normalize="sum",
                 concat=False):
        """
        
        :param df: 
        :param column_names: 
        :param edit_distance_func: 
        :param normalize: str. ``max``, ``sum``, ``total``, None
        :param concat: 
        """
        self.index_field = 'first_name'
        self.size = len(df)
        self.df = df
        self.column_names = column_names
        self.k = 1 if concat else len(column_names)
        self.x = defaultdict(list)
        self.func = edit_distance_func
        self.normalize = normalize
        self.max_dist = []
        self._init_first_name_index()

    def process_part(self, row_indexes=None):
        """
        
        :param row_indexes: list of indexes
        :return: ([int, int, ...], np.array)
        """

        max_dist = [-1] * self.k
        if row_indexes is None:
            row_indexes = range(self.size)

        for i in row_indexes:
            s1 = get_strings(self.df.iloc[i], self.column_names, concat=self.k == 1)
            first_name = self.df.iloc[i]['first_name']
            for j in self.first_name_indexes[first_name]:

                s2 = get_strings(self.df.iloc[j], self.column_names, self.k == 1)

                distance = []

                for i1 in range(self.k):
                    current_dist = self.func(s1[i1], s2[i1])
                    if self.normalize == "max":
                        max_d = max(len(s1[i1]), len(s2[i1]))
                        distance.append((max_d - current_dist) / max_d)
                    elif self.normalize == "sum":
                        max_d = len(s1[i1]) + len(s2[i1])
                        distance.append((max_d - current_dist) / max_d)
                    else:
                        distance.append(current_dist)

                    max_dist[i1] = max(max_dist[i1], current_dist)
                if (j, distance) not in self.x[i]:
                    self.x[i].append((j, distance.copy()))
                if (i, distance) not in self.x[j]:
                    self.x[j].append((i, distance.copy()))

        return max_dist, self.x

    def get_ids(self, njobs):
        r = self.size % njobs
        matrix = np.reshape(np.arange(0, self.size - r), (-1, njobs))
        matrix = matrix.transpose().tolist()
        for i in range(self.size - r, self.size):
            matrix[i % r].append(i)
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

        """
        Levenshtein distance calculation
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

        positions = list(filter(lambda i: self.max_dist[i] != 0, range(self.k)))
        if len(positions) != 0:
            if self.normalize == "total":
                for row_id in self.x.keys():
                    for i in range(len(self.x[row_id])):
                        for j in positions:
                            self.x[row_id][i][1][j] = (self.max_dist[j] - self.x[row_id][i][1][j]) / self.max_dist[j]
        return {
            'values': self.x,
            'max_dist': self.max_dist
        }

    def _init_first_name_index(self):
        self.df['first_name'] = self.df['first_name'].apply(remove_double_letters)
        self.df['last_name'] = self.df['last_name'].apply(remove_double_letters)
        self.first_name_indexes = defaultdict(list)
        for row_id, row in self.df.iterrows():
            first_name = row['first_name']
            last_name = row['last_name']
            list_valid_ids1 = self.df[self.df['first_name'].str.contains(first_name)].index.values.tolist()
            list_valid_ids2 = self.df[self.df['last_name'].str.contains(last_name)].index.values.tolist()

            self.first_name_indexes[first_name] = list(
                set(self.first_name_indexes[first_name] + list_valid_ids1 + list_valid_ids2))
