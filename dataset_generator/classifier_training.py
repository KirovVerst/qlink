import pandas as pd, numpy as np
import os
import pickle
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC

try:
    from conf import BASE_DIR
except:
    from conf_example import BASE_DIR

TRAIN_DATA_PATH = os.path.join(BASE_DIR, 'data', 'mockaroo', 'ml', 'train.csv')
CLASSIFIER_PATH = os.path.join(BASE_DIR, 'data', 'mockaroo', 'ml', 'classifier.pkl')
DEV_MODE = False


def development(data, target, classifier):
    x_train, x_test, y_train, y_test = train_test_split(data, target, test_size=0.2, random_state=42)
    classifier.fit(X=x_train, y=y_train)
    y_pred = classifier.predict(x_test)
    acc = np.mean(y_test == y_pred)
    print(acc)


def production(data, target, classifier):
    classifier.fit(data, target)
    with open(CLASSIFIER_PATH, 'wb') as output:
        pickle.dump(classifier, output, pickle.HIGHEST_PROTOCOL)


def get_ready_data(df):
    y = df['match']
    for column_name in ['first_name_len_1', 'first_name_len_2',
                        'last_name_len_1', 'last_name_len_2',
                        'father_len_1', 'father_len_2']:
        df[column_name].apply(lambda x: (x - np.mean(x)) / (np.max(x) - np.min(x)))

    df.drop(['match'], axis=1, inplace=True)
    return df, y


if __name__ == "__main__":
    dataframe = pd.read_csv(TRAIN_DATA_PATH)
    data, y = get_ready_data(dataframe)
    clf = SVC()

    if DEV_MODE:
        development(data=data, target=y, classifier=clf)
    else:
        production(data=data, target=y, classifier=clf)
