import pandas as pd
import datetime, ast
import os
from metrics import edit_distance, error_number
from preprocessing import remove_double_letters
from result_saving import write_duplicates, write_meta_data

START_TIME = datetime.datetime.now()
INITIAL_DATA_SIZE = 1000
LEVEL = 0.85

data = pd.read_csv('data/ready/data_{0}.csv'.format(INITIAL_DATA_SIZE))

N = len(data)
x = [[0] * N for _ in range(N)]

"""
Levenshtein distance calculation
"""
indexes = data.index.values
max_dist = -1
for i in range(N):
    s1 = data.iloc[i]['first_name'] + data.iloc[i]['last_name']
    s1 = remove_double_letters(s1)
    for j in range(i + 1, N):

        s2 = data.iloc[j]['first_name'] + data.iloc[j]['last_name']
        s2 = remove_double_letters(s2)

        d = edit_distance(s1, s2)

        if d > max_dist:
            max_dist = d
        x[i][j] = d
        x[j][i] = d

print("Levenshtein distances have been calculated")
"""
Levenshtein distance normalization
"""

for i in range(N):
    x[i] = x[i][:i + 1] + list(map(lambda y: (max_dist - y) / max_dist > LEVEL, x[i][i + 1:]))
    x[i][i] = False

print("Normalization has been done")

"""
Duplicate search
"""
predicted_indexes = list()

results = list()
processed = set()


def rec(i, row):
    processed.add(i)
    try:
        index = row[i + 1:].index(True)
        index += (i + 1)
        return [index] + rec(index, row)
    except Exception:
        return []


for i, row in enumerate(x):
    if i not in processed:
        duplicates = [i] + rec(i, row)
        results.append(duplicates)
print("Duplicates have been found")

"""
Error number calculation
"""
truth = []
with open('data/true_duplicates/data_{0}.txt'.format(INITIAL_DATA_SIZE), 'r') as f:
    for line in f.readlines():
        arr = ast.literal_eval(line[:-1])
        truth.append(arr)

n_errors = error_number(truth, results, N)
print("Error number has been calculated")
"""
Write the results
"""
t = START_TIME.strftime("%d-%m %H:%M:%S").replace(" ", "__")
folder_path = 'results/{0}/'.format(t)
os.mkdir(folder_path)  # TODO: try-catch

write_duplicates(folder_path, results)
write_meta_data(folder_path, N, n_errors, START_TIME)
print("Results have been saved")
