import pandas as pd
import datetime
import os, json
from multiprocessing import Pool
from metrics import get_errors, edit_distance_matrix
from duplicate_searching import predict_duplicates
from result_saving import write_meta_data, write_errors
from data_recieving import get_dataframe, get_true_duplicates

INITIAL_DATA_SIZE = 100
DOCUMENT_NUMBER = 12
LEVEL = 0.80

START_TIME = datetime.datetime.now()
START_TIME_STR = START_TIME.strftime("%d-%m %H:%M:%S").replace(" ", "__")
FOLDER_PATH = 'logs/{0}-{1}/'.format(START_TIME_STR, INITIAL_DATA_SIZE)

os.mkdir(FOLDER_PATH)
total_time = datetime.timedelta()
total_number_of_errors = 0


def func(document_index):
    current_time = datetime.datetime.now()
    data_kwargs = {'init_data_size': INITIAL_DATA_SIZE, 'document_index': document_index}

    data = get_dataframe(kwargs=data_kwargs)

    matrix = edit_distance_matrix(data, column_names=['first_name', 'last_name', 'father'])

    predicted_duplicates = predict_duplicates(matrix['values'], LEVEL)

    true_duplicates = get_true_duplicates(kwargs=data_kwargs)

    errors = get_errors(true_duplicates['items'], predicted_duplicates['items'])
    """
    Write the logs
    """
    document_folder_path = os.path.join(FOLDER_PATH, str(document_index))

    os.mkdir(document_folder_path)  # TODO: try-catch

    write_errors(document_folder_path, df=data, errors=errors['items'])
    time_delta = datetime.datetime.now() - current_time

    local_meta_data = {
        'dataset_size': len(data),
        'number_of_errors': errors['number_of_errors'],
        'time_delta': str(time_delta),
        'max_dist': matrix['max_dist'],
        'threshold': LEVEL
    }
    write_meta_data(os.path.join(document_folder_path, "logs.json"), local_meta_data)
    print("Dataset {0} is ready.".format(document_index + 1))
    return errors['number_of_errors'], time_delta


with Pool() as p:
    results = p.map(func, list(range(DOCUMENT_NUMBER)))

for errors, time in results:
    total_time += time
    total_number_of_errors += errors

meta_data = {
    "number_of_datasets": DOCUMENT_NUMBER,
    "init_dataset_size": INITIAL_DATA_SIZE,
    "average_time": str(total_time / DOCUMENT_NUMBER),
    "average_number_of_errors": total_number_of_errors / DOCUMENT_NUMBER,
    "total_time": str(datetime.datetime.now() - START_TIME)
}
write_meta_data(os.path.join(FOLDER_PATH, 'meta.json'), meta_data)
