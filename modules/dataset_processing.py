import numpy as np
import os

from collections import defaultdict

from pathos.multiprocessing import Pool
from modules.record_metrics import levenshtein_edit_distance
from modules.record_processing import get_strings


class EditDistanceMatrix(object):
    def __init__(self, df, column_names,
                 edit_distance_func=levenshtein_edit_distance,
                 normalize="total",
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
        self.x = np.zeros((self.size, self.size, self.k))
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

            for j in range(i + 1, self.size):
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

                self.x[i][j] = distance
                self.x[j][i] = distance

        return max_dist, np.array(self.x)

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
            distances.transpose()

            matrixes = list(map(lambda result: result[1], results))
            self.x = sum(matrixes)

            self.max_dist = list(map(lambda i: max(distances[i]), range(self.k)))

        if self.normalize == "total":
            for i in range(self.k):
                if self.max_dist[i] != 0:
                    self.x[:, :, i] = (self.max_dist[i] - self.x[:, :, i]) / self.max_dist[i]
                else:
                    self.x[:, :, i] = np.ones((self.size, self.size))

        return {
            'values': self.x,
            'max_dist': self.max_dist
        }

    def _init_first_name_index(self):
        self.first_name_indexes = defaultdict(list)
        for row_id, row in self.df.iterrows():
            first_name = row['first_name']
            list_valid_ids = self.df[self.df['first_name'].str.contains(first_name)].index.values.tolist()
            self.first_name_indexes[first_name] = list_valid_ids
