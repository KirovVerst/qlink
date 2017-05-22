import json

from modules.log_functions import start_message, finish_message
from modules.dataset_processing import EditDistanceMatrix


class MatrixCalculation:
    def __init__(self, dataframe, str_fields, date_fields, index_path, matrix_path, index_field, norm_matrix_path):
        self.dataframe = dataframe
        self.index_path = index_path
        self.matrix_path = matrix_path
        self.norm_matrix_path = norm_matrix_path
        self.index_field = index_field
        self.str_fields = str_fields
        self.date_fields = date_fields

    def create_matrix(self, njobs=-1):
        s = start_message('Matrix')
        matrix_generator = EditDistanceMatrix(dataframe=self.dataframe,
                                              str_column_names=self.str_fields,
                                              date_column_names=self.date_fields,
                                              index_path=self.index_path,
                                              index_field=self.index_field,
                                              normalize=True)
        matrix = matrix_generator.get(njobs)
        with open(self.matrix_path, 'w') as fp:
            json.dump(matrix, fp)

        matrix = matrix['values']
        with open(self.norm_matrix_path, 'w') as fp:
            json.dump(matrix, fp)

        finish_message(s)
