import pickle
import numpy as np

from dataset_generator.training_dataset import get_row
from dataset_generator.classifier_training import CLASSIFIER_PATH
from modules.dataset_receiving import Data
from modules.result_estimation import get_differences, get_accuracy


def get_classifier():
    with open(CLASSIFIER_PATH, 'rb') as f:
        clf = pickle.load(f)
    return clf


def get_raw_data(initial_size, document_index):
    kwargs = {'init_data_size': initial_size, 'document_index': document_index}
    data = Data(dataset_type='mockaroo', kwargs=kwargs)
    X = data.df.to_dict(orient='index')
    duplicates = data.true_duplicates
    return X, duplicates


def predict_duplicates(X, clf):
    results = []
    processed = set()
    ids = list(X.keys())
    for i in range(len(ids)):
        if i in processed:
            continue
        processed.add(i)
        duplicates = [ids[i]]
        row1 = X[ids[i]]
        for j in range(i + 1, len(ids)):
            if j in processed:
                continue
            processed.add(j)

            row2 = X[ids[j]]
            r = get_row([row1, row2])
            r = np.array(r).reshape(1, -1)
            prediction = clf.predict(r)
            if prediction == 1:
                duplicates.append(ids[j])
        results.append(duplicates)
    return results


if __name__ == "__main__":
    classifier = get_classifier()
    initial_size = 1000
    for i in range(1):
        X, true_duplicates = get_raw_data(initial_size, i)
        pred_duplicates = predict_duplicates(X, classifier)
        diff = get_differences(true_duplicates['items'], pred_duplicates)
        print("Dataset {}".format(i + 1))
        print("Number of errors: {}".format(diff['number_of_errors']))
        print("Accuracy: {}".format(get_accuracy(diff['number_of_errors'], len(X.keys()))))
