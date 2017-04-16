import datetime, os
import pprint

from collections import defaultdict

from modules.dataset_receiving import Data
from modules.duplicate_searching import Predictor
from modules.result_estimation import get_differences, get_accuracy
from modules.dataset_processing import EditDistanceMatrix
from modules.result_saving import Logger

try:
    from conf import BASE_DIR
except Exception as ex:
    from conf_example import BASE_DIR

INITIAL_DATA_SIZE = 100
DOCUMENT_NUMBER = 3
COLUMN_NAMES = ['first_name', 'last_name', 'father']
LEVELS = list(map(lambda x: [x / 100] * 3, range(75, 85)))
LIST_2_FLOAT = "norm"  # "norm", "sum"
RECORD_COMPARATOR = "and"  # "and", "or"
INDEX_FIELDS = ['first_name', 'last_name', 'father']  # None, ['first_name']
NJOBS = 1


def func(document_index):
    current_time = datetime.datetime.now()
    print("Dataset {0} was started: \t\t{1}".format(document_index + 1, current_time))

    data_kwargs = {'init_data_size': INITIAL_DATA_SIZE, 'document_index': document_index}

    data = Data(dataset_type="mockaroo", kwargs=data_kwargs)

    matrix = EditDistanceMatrix(data.df, column_names=COLUMN_NAMES,
                                concat=False, normalize="total", index_fields=INDEX_FIELDS)
    matrix_values = matrix.get(NJOBS)

    print("Matrix was calculated: \t\t{}".format(datetime.datetime.now()))

    results_grouped_by_level = dict()

    logger = Logger(FOLDER_PATH, dataset_index=document_index)

    predictor = Predictor(data=matrix_values['values'], levels=LEVELS,
                          list2float=LIST_2_FLOAT, comparator=RECORD_COMPARATOR, save_extra_data=True)

    predicted_duplicates = predictor.predict_duplicates(NJOBS)

    print("Duplicates were predicted: \t{}".format(datetime.datetime.now()))

    for duplicates in predicted_duplicates:
        duplicate_items = sorted(duplicates['items'], key=lambda x: min(x))
        errors = get_differences(data.true_duplicates['items'], duplicate_items)
        errors['level'] = duplicates['level']
        errors['extra_data'] = duplicates['extra_data']
        level_str = str(errors['level'])
        results_grouped_by_level[level_str] = dict(errors=errors['number_of_errors'],
                                                   accuracy=get_accuracy(n_errors=errors['number_of_errors'],
                                                                         n_records=len(data.df)))

        logger.save_errors(df=data.df, errors=errors)

        logger.save_duplicates(duplicates, errors['level'])

    time_delta = datetime.datetime.now() - current_time

    current_meta_data = {
        'dataset_size': len(data.df),
        'results': results_grouped_by_level,
        'time_delta': str(time_delta),
        'max_dist': str(matrix_values['max_dist']),
        'normalize': str(matrix.normalize),
        "concat": str(matrix.k == 1)
    }
    pprint.pprint(current_meta_data['results'])
    print("Dataset {0} is ready: \t\t{1}".format(document_index + 1, datetime.datetime.now()))
    print("Time delta: {0}\n\n".format(str(time_delta)))

    logger.save_data(data=current_meta_data)

    return results_grouped_by_level


if __name__ == "__main__":
    results = []
    START_TIME = datetime.datetime.now()
    START_TIME_STR = START_TIME.strftime("%d_%m_%H_%M_%S").replace(" ", "__")
    FOLDER_PATH = os.path.join(BASE_DIR, 'logs', '{0}_{1}'.format(START_TIME_STR, INITIAL_DATA_SIZE))

    total_time = datetime.timedelta()
    total_number_of_errors = 0

    average_result = defaultdict(dict)
    for i in range(DOCUMENT_NUMBER):
        result = func(i)
        for level, value in result.items():
            try:
                average_result[level]['errors'] += value['errors']
                average_result[level]['accuracy'] += value['accuracy']
            except:
                average_result[level]['errors'] = value['errors']
                average_result[level]['accuracy'] = value['accuracy']

    print("-" * 80)

    for key in average_result.keys():
        average_result[key]['accuracy'] /= DOCUMENT_NUMBER
        average_result[key]['errors'] /= DOCUMENT_NUMBER

    meta_data = {
        "number_of_datasets": DOCUMENT_NUMBER,
        "init_dataset_size": INITIAL_DATA_SIZE,
        "dataset_fields": COLUMN_NAMES,
        "levels": LEVELS,
        "list2float_function": LIST_2_FLOAT,
        "record_comparision_function": RECORD_COMPARATOR,
        "average_results": dict(average_result.items()),
        "total_time": str(datetime.datetime.now() - START_TIME),
    }
    pprint.pprint(meta_data)
    logger = Logger(FOLDER_PATH)
    logger.save_data(data=meta_data)
