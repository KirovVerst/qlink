import pandas as pd
import datetime, ast
import os
from multiprocessing import Pool
from metrics import error_number, edit_distance_matrix
from duplicate_searching import get_duplicates
from result_saving import write_meta_data, write_errors

INITIAL_DATA_SIZE = 100
DOCUMENT_NUMBER = 1
LEVEL = 0.80

START_TIME = datetime.datetime.now()
START_TIME_STR = START_TIME.strftime("%d-%m %H:%M:%S").replace(" ", "__")
FOLDER_PATH = 'logs/{0}-{1}/'.format(START_TIME_STR, INITIAL_DATA_SIZE)
os.mkdir(FOLDER_PATH)  # TODO: try-catch
total_time = datetime.timedelta()
total_number_error = 0


def func(document_index):
    current_time = datetime.datetime.now()
    # print("{0} document".format(document_index + 1))
    data = pd.read_csv('data/ready/{0}/data_{1}.csv'.format(INITIAL_DATA_SIZE, document_index))

    x, max_dist = edit_distance_matrix(data, columns=['first_name', 'last_name', 'father'])

    results = get_duplicates(x, LEVEL)
    """
    Error number calculation
    """
    truth = []
    with open('data/true_duplicates/{0}/data_{1}.txt'.format(INITIAL_DATA_SIZE, document_index), 'r') as f:
        for line in f.readlines():
            arr = ast.literal_eval(line[:-1])
            truth.append(arr)

    number_of_errors, errors = error_number(truth, results)
    # print("Error number has been calculated")
    """
    Write the logs
    """
    document_folder_path = os.path.join(FOLDER_PATH, str(document_index))
    os.mkdir(document_folder_path)  # TODO: try-catch

    write_errors(document_folder_path, data=data, errors=errors)
    delta_time = datetime.datetime.now() - current_time

    local_meta_data = {
        'datset_size': len(data),
        'number_of_errors': number_of_errors,
        'delta_time': str(delta_time),
        'max_dist': max_dist,
        'threshold': LEVEL
    }
    log_path = os.path.join(document_folder_path, 'log.json')
    write_meta_data(log_path, local_meta_data)
    print("Dataset {0} is ready.".format(document_index + 1))
    return number_of_errors, delta_time


with Pool() as p:
    results = p.map(func, list(range(DOCUMENT_NUMBER)))

for errors, time in results:
    total_time += time
    total_number_error += errors

meta_data = {
    "number_of_datasets": DOCUMENT_NUMBER,
    "init_dataset_size": INITIAL_DATA_SIZE,
    "average_time": str(total_time / DOCUMENT_NUMBER),
    "average_number_of_errors": total_number_error / DOCUMENT_NUMBER,
    "total_time": str(datetime.datetime.now() - START_TIME)
}
write_meta_data(os.path.join(FOLDER_PATH, 'meta.json'), meta_data)
