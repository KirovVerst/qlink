import datetime, os
import pprint

from collections import defaultdict

from modules.dataset_receiving import Data
from modules.result_estimation import get_differences, get_accuracy
from modules.result_saving import Logger
from indexation import Indexation
from matrix_calculation import MatrixCalculation
from duplicate_searching import DuplicateSearching

try:
    from conf import BASE_DIR
except Exception as ex:
    from conf_example import BASE_DIR

INITIAL_DATA_SIZE = 1000
DOCUMENT_NUMBER = 1
COLUMN_NAMES = ['first_name', 'last_name', 'father']
LEVELS = list(map(lambda l: [l / 100] * 3 + [0.9], range(70, 91, 2)))
LIST_2_FLOAT = "norm"  # "norm", "sum"
RECORD_COMPARATOR = "and"  # "and", "or"
FIELDS = ['first_name', 'last_name', 'father_name']  # None, ['first_name']
NJOBS = 1


def func(document_index):
    current_time = datetime.datetime.now()
    print("Dataset {0} was started: \t\t{1}".format(document_index + 1, current_time))

    data_kwargs = {'document_index': document_index}

    data = Data(dataset_type="miac_test", kwargs=data_kwargs)
    data.df.fillna(value='', inplace=True)

    mode = 'letters'

    indexator = Indexation(dataframe=data.df,
                           index_field='last_name',
                           mode=mode,
                           index_output_path='index-{}-{}.json'.format(document_index, mode))
    # indexator.create_index_dict(njobs=1)

    calculator = MatrixCalculation(dataframe=data.df,
                                   index_path=indexator.output_path_json,
                                   index_field='last_name',
                                   matrix_path='matrix-{}-{}.json'.format(document_index, mode),
                                   norm_matrix_path='matrix-norm-{}-{}.json'.format(document_index, mode),
                                   str_fields=FIELDS,
                                   date_fields=['birthday'])
    # calculator.create_matrix()

    searcher = DuplicateSearching(dataframe=data.df,
                                  norm_matrix_path=calculator.norm_matrix_path,
                                  duplicates_path='duplicates-{}-{}-max.json'.format(document_index, mode),
                                  mode='all')

    searcher.search_duplicates(levels=LEVELS)

    results_grouped_by_level = dict()

    logger = Logger(FOLDER_PATH, dataset_index=document_index)

    predicted_duplicates = searcher.duplicates_list

    for duplicates in predicted_duplicates:
        duplicate_items = list(map(lambda l: list(map(lambda x: int(x), l)), duplicates['items']))
        duplicate_items = sorted(duplicate_items, key=lambda x: min(x))
        errors = get_differences(true=data.true_duplicates['items'], predict=duplicate_items)
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
        'normalize': str(True),
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
