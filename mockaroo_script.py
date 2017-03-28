import datetime, os
from multiprocessing import Pool

from modules.dataset_receiving import Data
from modules.duplicate_searching import predict_duplicates, get_differences
from modules.dataset_processing import edit_distance_matrix

from modules.result_saving import Logger

INITIAL_DATA_SIZE = 100
DOCUMENT_NUMBER = 1
LEVEL = 0.80

START_TIME = datetime.datetime.now()
START_TIME_STR = START_TIME.strftime("%d-%m %H:%M:%S").replace(" ", "__")
FOLDER_PATH = os.path.join('logs', '{0}-{1}'.format(START_TIME_STR, INITIAL_DATA_SIZE))

total_time = datetime.timedelta()
total_number_of_errors = 0


def func(document_index):
    current_time = datetime.datetime.now()
    data_kwargs = {'init_data_size': INITIAL_DATA_SIZE, 'document_index': document_index}

    data = Data(dataset_type="mockaroo", kwargs=data_kwargs)

    matrix = edit_distance_matrix(data.df, column_names=['first_name', 'last_name', 'father'])

    predicted_duplicates = predict_duplicates(matrix['values'], LEVEL)

    errors = get_differences(data.true_duplicates['items'], predicted_duplicates['items'])

    logger = Logger(FOLDER_PATH, dataset_index=document_index)

    logger.save_errors(df=data.df, errors=errors['items'])

    logger.save_duplicates(predicted_duplicates)

    time_delta = datetime.datetime.now() - current_time

    local_meta_data = {
        'dataset_size': len(data.df),
        'number_of_errors': errors['number_of_errors'],
        'time_delta': str(time_delta),
        'max_dist': matrix['max_dist'],
        'threshold': LEVEL
    }
    logger.save_data(data=local_meta_data)
    print("Dataset {0} is ready.".format(document_index + 1))
    return errors['number_of_errors'], time_delta


with Pool(1) as p:
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
logger = Logger(FOLDER_PATH)
logger.save_data(data=meta_data)
