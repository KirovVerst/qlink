import numpy as np
import os
import json

from collections import defaultdict
from pathos.multiprocessing import Pool

from modules.record_metrics import levenshtein_edit_distance
from modules.record_processing import get_strings


class EditDistanceMatrix(object):
    def __init__(self, dataframe, str_column_names, date_column_names, index_path, index_field, normalize,
                 edit_distance_function=levenshtein_edit_distance):
        """
        
        :param dataframe: 
        :param str_column_names: 
        :param edit_distance_function: 
        :param normalize: bool
        """
        self.size = len(dataframe)
        self.dataframe = dataframe
        self.str_column_names = str_column_names
        self.date_column_names = date_column_names
        self.x = defaultdict(list)
        self.edit_distance = edit_distance_function
        self.normalize = normalize
        self.max_dist = []
        self.fields = self.str_column_names + self.date_column_names
        self.index_field = index_field
        # index dictionary
        with open(index_path, 'r') as fp:
            self.index_dict = json.load(fp)

    def process_part(self, row_indexes=None):
        """
        
        :param row_indexes: list of indexes
        :return: ([int, int, ...], np.array)
        """

        max_distances = [-1] * len(self.fields)
        if row_indexes is None:
            row_indexes = self.dataframe.index.values.tolist()
        count = 0
        total_count = len(row_indexes)

        for i in row_indexes:
            self.x[i] = []

            if self.dataframe.loc[i][self.index_field] != '':
                index_field_values = self.dataframe.loc[i][self.index_field].split('-')
            else:
                continue

            s1 = get_strings(self.dataframe.loc[i], self.str_column_names)

            for field in self.date_column_names:
                s1.append([self.dataframe.loc[i][field]])

            for index_field_value in index_field_values:
                if index_field_value in self.index_dict:
                    available_keys = list(set(self.index_dict[index_field_value]))

                try:
                    available_keys.remove(i)
                except Exception:
                    pass

                for j in available_keys:

                    s2 = get_strings(self.dataframe.loc[j], self.str_column_names)
                    for field in self.date_column_names:
                        s2.append([self.dataframe.loc[j][field]])

                    distances = []

                    for k, field_name in enumerate(self.fields):
                        if field_name == self.index_field:
                            list_values_1 = [index_field_value]
                        else:
                            list_values_1 = list(filter(lambda x: len(x) > 0, s1[k]))

                        list_values_2 = list(filter(lambda x: len(x) > 0, s2[k]))
                        field_distance = []
                        for v1 in list_values_1:
                            for v2 in list_values_2:
                                current_distance = self.edit_distance(v1, v2)
                                field_distance.append([current_distance, len(v1), len(v2)])

                        if len(field_distance) == 0:
                            distances.append(None)
                            continue
                        else:
                            field_distance = min(field_distance, key=lambda dist: dist[0])
                        if self.normalize:
                            max_distance = max([field_distance[1], field_distance[2]])
                            distances.append((max_distance - field_distance[0]) / max_distance)
                        else:
                            distances.append(field_distance[0])

                        max_distances[k] = max(max_distances[k], field_distance[0])

                    if [None] not in s1 and (j, distances) not in self.x[i]:
                        self.x[i].append((j, distances.copy()))

                    if [None] not in s2 and (i, distances) not in self.x[j]:
                        self.x[j].append((i, distances.copy()))
            count += 1
            if count % 500 == 0:
                print(os.getpid(), ' : ', round(count / total_count, 3))
        return max_distances, self.x

    def get_ids(self, njobs):
        r = self.size % njobs
        indexes = self.dataframe.index.values.tolist()
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

            self.max_dist = list(map(lambda i: max(distances[i]), range(len(self.fields))))

        return {
            'values': self.x,
            'max_dist': list(map(int, self.max_dist))
        }
