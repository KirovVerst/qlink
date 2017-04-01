import datetime, os
from pathos.multiprocessing import Pool
from modules.dataset_receiving import Data
from modules.duplicate_searching import predict_duplicates
from modules.result_estimation import get_differences
from modules.dataset_processing import EditDistanceMatrix
from modules.result_saving import Logger

if __name__ == "__main__":
    dataset_type = "sql"
    level = [0.80, 0.80, 0.80]
    name = "data_0"
    kwargs = {
        "dialect": "mysql",
        "user": "username",
        "password": "password",
        "host": "localhost:3306",
        "db_name": "dataset",
        "query": "SELECT * FROM data_0"
    }

    current_time = datetime.datetime.now()
    FOLDER_PATH = os.path.join('logs', '{0}-{1}'.format(current_time.strftime("%d-%m %H:%M:%S").replace(" ", "__"),
                                                        name))
    data = Data(dataset_type=dataset_type, kwargs=kwargs)

    matrix = EditDistanceMatrix(data.df, column_names=['first_name', 'last_name', 'father'], concat=True)
    matrix = matrix.get()

    predicted_duplicates = predict_duplicates(matrix['values'], level)

    logger = Logger(FOLDER_PATH, dataset_index=0)

    logger.save_duplicates(predicted_duplicates)

    time_delta = datetime.datetime.now() - current_time

    del kwargs['password']
    current_meta_data = {
        'dataset_size': len(data.df),
        'time_delta': str(time_delta),
        'max_dist': matrix['max_dist'],
        'threshold': level,
        'dataset_info': kwargs,
    }
    logger.save_data(data=current_meta_data)
    print("Dataset {0} is ready.".format(1))
