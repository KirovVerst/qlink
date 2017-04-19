import pandas as pd, numpy as np
import os
from sklearn.model_selection import train_test_split
from sklearn.linear_model import SGDClassifier

try:
    from conf import BASE_DIR
except:
    from conf_example import BASE_DIR

TRAIN_DATA_PATH = os.path.join(BASE_DIR, 'data', 'mockaroo', 'ml', 'train.csv')

if __name__ == "__main__":
    data = pd.read_csv(TRAIN_DATA_PATH)
    y = data['match']
    for column_name in ['first_name_len_1', 'first_name_len_2',
                        'last_name_len_1', 'last_name_len_2',
                        'father_len_1', 'father_len_2']:
        data[column_name].apply(lambda x: (x - np.mean(x)) / (np.max(x) - np.min(x)))

    data.drop(['match'], axis=1, inplace=True)
    X_train, X_test, y_train, y_test = train_test_split(data, y, test_size=0.2, random_state=42)
    clf = SGDClassifier()
    clf.fit(X=X_train, y=y_train)
    y_pred = clf.predict(X_test)
    acc = np.mean(y_test == y_pred)
    print(acc)
