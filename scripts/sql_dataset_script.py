import datetime, os

from modules.dataset_receiving import Data
from modules.duplicate_searching import predict_duplicates
from modules.dataset_processing import EditDistanceMatrix
from modules.result_saving import Logger

try:
    from conf import DATABASE
except Exception:
    from conf_example import DATABASE

if __name__ == "__main__":
    dataset_type = "sql"
    level = [0.80, 0.80, 0.80]
    name = "data_0"

    current_time = datetime.datetime.now()
    current_time_str = current_time.strftime("%d-%m %H:%M:%S")
    print("Start time: {0}".format(current_time_str))
    FOLDER_PATH = os.path.join('logs', '{0}-{1}'.format(current_time_str.replace(" ", "__"),
                                                        name))
    data = Data(dataset_type=dataset_type, kwargs=DATABASE['default'])

    matrix = EditDistanceMatrix(data.df, column_names=['first_name', 'last_name', 'father'], concat=True)
    matrix = matrix.get()

    predicted_duplicates = predict_duplicates(matrix['values'], level)

    logger = Logger(FOLDER_PATH, dataset_index=0)

    logger.save_duplicates(predicted_duplicates)

    time_delta = datetime.datetime.now() - current_time

    if 'password' in DATABASE['default']:
        del DATABASE['default']['password']

    current_meta_data = {
        'dataset_size': len(data.df),
        'time_delta': str(time_delta),
        'max_dist': matrix['max_dist'],
        'threshold': level,
        'dataset_info': DATABASE['default'],
    }
    logger.save_data(data=current_meta_data)
    print("Dataset {0} is ready.".format(1))
    print("Time: {0}".format(current_meta_data['time_delta']))
