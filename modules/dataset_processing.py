import numpy as np
import os
from pathos.multiprocessing import Pool
from modules.record_metrics import levenshtein_edit_distance
from modules.record_processing import get_strings


class EditDistanceMatrix(object):
    def __init__(self, df, column_names,
                 edit_distance_func=levenshtein_edit_distance,
                 normalize=True,
                 concat=False):
        self.size = len(df)
        self.df = df
        self.column_names = column_names
        self.k = 1 if concat else len(column_names)
        self.x = np.zeros((self.size, self.size, self.k))
        self.func = edit_distance_func
        self.normalize = normalize
        self.max_dist = []

    def process_part(self, row_indexes=None):
        max_dist = [-1] * self.k
        if row_indexes is None:
            row_indexes = range(self.size)

        for i in row_indexes:
            s1 = get_strings(self.df.iloc[i], self.column_names, self.k == 1)
            for j in range(i + 1, self.size):
                s2 = get_strings(self.df.iloc[j], self.column_names, self.k == 1)
                d = []
                for i1 in range(self.k):
                    d.append(self.func(s1[i1], s2[i1]))

                self.x[i][j] = d
                self.x[j][i] = d

                for i1 in range(len(d)):
                    if d[i1] > max_dist[i1]:
                        max_dist[i1] = d[i1]
        return max_dist, np.array(self.x)

    def get_row_indexes(self, njobs):
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
            indexes = self.get_row_indexes(njobs)

            with Pool(njobs) as p:
                results = list(p.map(self.process_part, indexes))

            for result in results:
                self.x += result[1]
            for i in range(self.k):
                self.max_dist.append(max(results, key=lambda k: k[0][i])[0][i])

        """
        Levenshtein distance normalization
        """
        if self.normalize:
            for i in range(self.k):
                if self.max_dist[i] != 0:
                    self.x[:, :, i] = (self.max_dist[i] - self.x[:, :, i]) / self.max_dist[i]
                else:
                    self.x[:, :, i] = np.ones((self.size, self.size))
            for j in range(self.size):
                self.x[j, j, :] = 0
        return {
            'values': self.x,
            'max_dist': self.max_dist
        }
