import datetime, os
import pprint

from collections import defaultdict

from modules.dataset_receiving import Data
from modules.duplicate_searching import predict_duplicates
from modules.result_estimation import get_differences
from modules.dataset_processing import EditDistanceMatrix
from modules.result_saving import Logger

try:
    from conf import BASE_DIR
except Exception as ex:
    from conf_example import BASE_DIR

INITIAL_DATA_SIZE = 3000
DOCUMENT_NUMBER = 1


def func(document_index):
    current_time = datetime.datetime.now()
    print("Dataset {0} is started: {1}".format(document_index + 1, current_time.strftime("%d-%m %H:%M:%S")))

    data_kwargs = {'init_data_size': INITIAL_DATA_SIZE, 'document_index': document_index}

    data = Data(dataset_type="mockaroo", kwargs=data_kwargs)

    matrix = EditDistanceMatrix(data.df, column_names=['first_name', 'last_name', 'father'],
                                concat=False, normalize="total")
    matrix_values = matrix.get()

    results_grouped_by_level = dict()

    logger = Logger(FOLDER_PATH, dataset_index=document_index)

    for level in range(70, 85):
        level /= 100
        predicted_duplicates = predict_duplicates(matrix_values['values'], [level] * 3)

        errors = get_differences(data.true_duplicates['items'], predicted_duplicates['items'])
        errors['level'] = level
        results_grouped_by_level[level] = errors['number_of_errors']

        logger.save_errors(df=data.df, errors=errors)

        logger.save_duplicates(predicted_duplicates, level)

    time_delta = datetime.datetime.now() - current_time

    current_meta_data = {
        'dataset_size': len(data.df),
        'results': results_grouped_by_level,
        'time_delta': str(time_delta),
        'max_dist': matrix_values['max_dist'],
        'normalize': str(matrix.normalize),
        "concat": str(matrix.k == 1)
    }

    print("Dataset {0} is ready: {1}".format(document_index + 1, datetime.datetime.now().strftime("%d-%m %H:%M:%S")))
    print("Time delta: {0}".format(str(time_delta)))
    pprint.pprint(current_meta_data['results'])

    logger.save_data(data=current_meta_data)

    return results_grouped_by_level


if __name__ == "__main__":
    results = []
    START_TIME = datetime.datetime.now()
    START_TIME_STR = START_TIME.strftime("%d_%m_%H_%M_%S").replace(" ", "__")
    FOLDER_PATH = os.path.join(BASE_DIR, 'logs', '{0}_{1}'.format(START_TIME_STR, INITIAL_DATA_SIZE))

    total_time = datetime.timedelta()
    total_number_of_errors = 0
    total_result = defaultdict(int)
    for i in range(DOCUMENT_NUMBER):
        result = func(i)
        for level, value in result.items():
            total_result[level] += value

    for key in total_result:
        total_result[key] /= DOCUMENT_NUMBER

    meta_data = {
        "number_of_datasets": DOCUMENT_NUMBER,
        "init_dataset_size": INITIAL_DATA_SIZE,
        "average_number_of_errors": total_result,
        "total_time": str(datetime.datetime.now() - START_TIME),
    }
    pprint.pprint(meta_data)
    logger = Logger(FOLDER_PATH)
    logger.save_data(data=meta_data)
