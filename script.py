import pandas as pd
import numpy as np


def distance(a, b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        a, b = b, a
        n, m = m, n

    current_row = range(n + 1)
    for i in range(1, m + 1):
        previous_row, current_row = current_row, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete, change = previous_row[j] + 1, current_row[j - 1] + 1, previous_row[j - 1]
            if a[j - 1] != b[i - 1]:
                change += 1
            current_row[j] = min(add, delete, change)

    return current_row[n]


data = pd.read_csv('data/ready/data_1.csv')

x = []
indexes = data.index.values
max_dist = -1
for i in indexes:
    measures = []
    for j in indexes:
        d = distance(data.get_value(i, 'first_name'), data.get_value(j, 'first_name'))
        d += distance(data.get_value(i, 'last_name'), data.get_value(j, 'last_name'))
        if d > max_dist:
            max_dist = d
        measures.append(d)
    x.append(measures)

print("Levenshtein distances have been calculated")

for i in range(len(x)):
    for j in range(len(x[i])):
        x[i][j] = float(max_dist - x[i][j]) / max_dist
    x[i][i] = -1

print("Metrics have been calculated")

predicted_indexes = list()

LEVEL = 0.9
predicted_values = {'first_name': [], 'last_name': [], 'id': [], 'line': []}
for row in x:
    max_index, max_value = max(enumerate(row), key=lambda p: p[1])
    if max_value > LEVEL:
        predicted_values['line'].append(indexes[max_index])
        predicted_values['id'].append(data.loc[indexes[max_index]]['original_id'])
        predicted_values['first_name'].append(data.loc[indexes[max_index]]['first_name'])
        predicted_values['last_name'].append(data.loc[indexes[max_index]]['last_name'])
    else:
        predicted_values['line'].append(None)
        predicted_values['id'].append(None)
        predicted_values['first_name'].append(None)
        predicted_values['last_name'].append(None)

data = data.assign(pred_id=predicted_values['id'],
                   pred_first_name=predicted_values['first_name'],
                   pred_last_name=predicted_values['last_name'],
                   line=predicted_values['line'])

data = data[pd.notnull(data['pred_id'])]
data.to_csv('results/data_1.csv', index=False)

acc = np.mean(data[:]['original_id'].values == data[:]['pred_id'].values)

with open('results/accurancy', 'w') as f:
    f.write("{0}".format(acc))
