import pandas as pd
import datetime, ast
import os
from multiprocessing import Pool
from metrics import error_number, edit_distance_matrix
from duplicate_searching import get_duplicates
from result_saving import write_meta_data, write_errors

INITIAL_DATA_SIZE = 100
DOCUMENT_NUMBER = 4
LEVEL = 0.865

START_TIME = datetime.datetime.now()
START_TIME_STR = START_TIME.strftime("%d-%m %H:%M:%S").replace(" ", "__")
FOLDER_PATH = 'results/{0}-{1}/'.format(START_TIME_STR, INITIAL_DATA_SIZE)
os.mkdir(FOLDER_PATH)  # TODO: try-catch
total_time = datetime.timedelta()
total_number_error = 0


def func(document_index):
    current_time = datetime.datetime.now()
    # print("{0} document".format(document_index + 1))
    data = pd.read_csv('data/ready/{0}/data_{1}.csv'.format(INITIAL_DATA_SIZE, document_index))

    x = edit_distance_matrix(data, columns=['first_name', 'last_name', 'father'])

    results = get_duplicates(x)
    """
    Error number calculation
    """
    truth = []
    with open('data/true_duplicates/{0}/data_{1}.txt'.format(INITIAL_DATA_SIZE, document_index), 'r') as f:
        for line in f.readlines():
            arr = ast.literal_eval(line[:-1])
            truth.append(arr)

    n_errors, errors = error_number(truth, results)
    # print("Error number has been calculated")
    """
    Write the results
    """
    document_folder_path = os.path.join(FOLDER_PATH, str(document_index))
    os.mkdir(document_folder_path)  # TODO: try-catch

    write_errors(document_folder_path, data=data, errors=errors)
    delta_time = datetime.datetime.now() - current_time
    write_meta_data(document_folder_path, len(data), n_errors, delta_time)
    # print("Results have been saved\n")
    return n_errors, delta_time


with Pool() as p:
    results = p.map(func, list(range(DOCUMENT_NUMBER)))

for errors, time in results:
    total_time += time
    total_number_error += errors

with open(os.path.join(FOLDER_PATH, 'meta.txt'), 'w') as f:
    f.write("Dataset count: {0}\n".format(DOCUMENT_NUMBER))
    f.write("Initial dataset size: {0}\n".format(INITIAL_DATA_SIZE))
    f.write("Average time: {0}\n".format(total_time / DOCUMENT_NUMBER))
    f.write("Average error number: {0}\n".format(total_number_error / DOCUMENT_NUMBER))

print("Total time: {0}".format(datetime.datetime.now() - START_TIME))
