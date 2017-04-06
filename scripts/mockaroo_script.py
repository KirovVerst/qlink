import datetime, os

from modules.dataset_receiving import Data
from modules.duplicate_searching import predict_duplicates
from modules.result_estimation import get_differences
from modules.dataset_processing import EditDistanceMatrix
from modules.result_saving import Logger

try:
    from conf import BASE_DIR
except:
    from conf_example import BASE_DIR

INITIAL_DATA_SIZE = 100
DOCUMENT_NUMBER = 12


def func(document_index, level):
    current_time = datetime.datetime.now()
    data_kwargs = {'init_data_size': INITIAL_DATA_SIZE, 'document_index': document_index}

    data = Data(dataset_type="mockaroo", kwargs=data_kwargs)

    matrix = EditDistanceMatrix(data.df, column_names=['first_name', 'last_name', 'father'],
                                concat=False, normalize="total")
    matrix_values = matrix.get()

    predicted_duplicates = predict_duplicates(matrix_values['values'], level)

    errors = get_differences(data.true_duplicates['items'], predicted_duplicates['items'])

    logger = Logger(FOLDER_PATH, dataset_index=document_index)

    logger.save_errors(df=data.df, errors=errors['items'])

    logger.save_duplicates(predicted_duplicates)

    time_delta = datetime.datetime.now() - current_time

    current_meta_data = {
        'dataset_size': len(data.df),
        'number_of_errors': errors['number_of_errors'],
        'time_delta': str(time_delta),
        'max_dist': matrix_values['max_dist'],
        'threshold': level,
        'normalize': str(matrix.normalize),
        "concat": True if matrix.k == 1 else True
    }
    logger.save_data(data=current_meta_data)
    print("Dataset {0} is ready.".format(document_index + 1))
    return errors['number_of_errors'], time_delta


if __name__ == "__main__":
    results = []
    for k in [0.7]:
        START_TIME = datetime.datetime.now()
        START_TIME_STR = START_TIME.strftime("%d_%m_%H_%M_%S").replace(" ", "__")
        FOLDER_PATH = os.path.join(BASE_DIR, 'logs', '{0}_{1}'.format(START_TIME_STR, INITIAL_DATA_SIZE))

        total_time = datetime.timedelta()
        total_number_of_errors = 0
        print("Threshold: {0}".format(k))
        for i in range(DOCUMENT_NUMBER):
            results.append(tuple(func(i, [k] * 3)))

        for errors, time in results:
            total_time += time
            total_number_of_errors += errors

        meta_data = {
            "number_of_datasets": DOCUMENT_NUMBER,
            "init_dataset_size": INITIAL_DATA_SIZE,
            "average_time": str(total_time / DOCUMENT_NUMBER),
            "average_number_of_errors": total_number_of_errors / DOCUMENT_NUMBER,
            "total_time": str(datetime.datetime.now() - START_TIME),
        }
        print(meta_data)
        logger = Logger(FOLDER_PATH)
        logger.save_data(data=meta_data)
