import numpy as np
import os
from pathos.multiprocessing import Pool
from modules.record_metrics import levenshtein_edit_distance
from modules.record_processing import remove_double_letters


class EditDistanceMatrix(object):
    def __init__(self, df, column_names, edit_distance_func=levenshtein_edit_distance, normalize=True):
        self.size = len(df)
        self.df = df
        self.x = np.zeros((self.size, self.size))
        self.column_names = column_names
        self.func = edit_distance_func
        self.normalize = normalize

    def process_part(self, row_indexes=None):
        max_dist = -1
        if row_indexes is None:
            row_indexes = range(self.size)
        for i in row_indexes:
            s1 = ""
            for column_name in self.column_names:
                s1 += remove_double_letters(self.df.iloc[i][column_name])

            for j in range(i + 1, self.size):
                s2 = ""
                for column_name in self.column_names:
                    s2 += remove_double_letters(self.df.iloc[j][column_name])

                d = self.func(s1, s2)
                if d > max_dist:
                    max_dist = d
                self.x[i][j] = d
                self.x[j][i] = d
        return max_dist, np.array(self.x)

    def get(self, njobs=-1):
        """
            
            :param  njobs: number of threads. If njobs is -1, all threads will be used. 
                    njobs must be 1 if the method 'get' is calling from not main process 
                    else the following exception will be raised:
                    "AssertionError: daemonic processes are not allowed to have children"
            :return: 
            {
                values: list of list of int. [[12, 34], [34, 45]]. shape = N*N, where N is len(df).
                max_dist: int. max distance that is contained in the matrix
            }
            """

        """
        Levenshtein distance calculation
        """

        if njobs == 1:
            max_dist, x = self.process_part()
            self.x = x.tolist()
            self.max_dist = max_dist
        else:
            if njobs == -1:
                njobs = os.cpu_count()
            r = self.size % njobs
            matrix = np.reshape(np.arange(0, self.size - r), (-1, njobs))
            matrix = matrix.transpose().tolist()
            for i in range(self.size - r, self.size):
                matrix[i % r].append(i)

            with Pool(njobs) as p:
                results = list(p.map(self.process_part, matrix))

            for result in results:
                self.x += result[1]
            self.x = self.x.tolist()
            self.max_dist = max(results, key=lambda k: k[0])[0]

        """
        Levenshtein distance normalization
        """
        if self.normalize:
            if self.max_dist != 0:
                for i in range(self.size):
                    self.x[i] = list(map(lambda y: (self.max_dist - y) / self.max_dist, self.x[i]))
                    self.x[i][i] = 0
            else:
                self.x = [[1] * self.size for _ in range(self.size)]
                for i in range(self.size):
                    self.x[i][i] = 0
        return {
            'values': self.x,
            'max_dist': self.max_dist
        }
