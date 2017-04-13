import os, datetime
import numpy as np
import dedupe

from modules.result_estimation import get_differences, get_accuracy
from modules.dataset_receiving import Data

try:
    from conf import BASE_DIR
except:
    from conf_example import BASE_DIR

INITIAL_DATA_SIZE = 100
DOCUMENT_INDEX = 1

FIELDS = [
    {'field': 'first_name', 'type': 'String'},
    {'field': 'last_name', 'type': 'String'},
    {'field': 'father', 'type': 'String'},
    {'field': 'gender', 'type': 'Categorical', 'categories': ['M', 'F']}
]

output_file = os.path.join(BASE_DIR, "logs",
                           "output-{0}.csv".format(datetime.datetime.now().strftime("%d-%m-%y_%H_%M_%S")))

ACTIVE_LEARNING = True

settings_file = os.path.join(BASE_DIR, "data", "mockaroo", "dedupe", "train", 'learned_settings')
training_file = os.path.join(BASE_DIR, "data", "mockaroo", "dedupe", "train", "training_dataset.json")

print("importing data")

kwargs = {'init_data_size': INITIAL_DATA_SIZE, 'document_index': DOCUMENT_INDEX}

data = Data(dataset_type="mockaroo", kwargs=kwargs)
y = data.df['original_id']
del data.df['original_id']

data_d = data.df.to_dict(orient='index')

deduper = dedupe.Dedupe(FIELDS)

deduper.sample(data_d, 1000)

if os.path.exists(training_file):
    print('reading labeled examples from ', training_file)
    with open(training_file, 'rb') as f:
        deduper.readTraining(f)
else:
    ACTIVE_LEARNING = True

if ACTIVE_LEARNING:
    print('starting active labeling...')

    dedupe.consoleLabel(deduper)
    deduper.train()

    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    with open(settings_file, 'wb') as sf:
        deduper.writeSettings(sf)
else:
    deduper.train()

threshold = deduper.threshold(data_d, recall_weight=1)

print("Clustering")

clustered_dupes = deduper.match(data_d, threshold)

cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    for record_id, score in zip(id_set, scores):
        cluster_membership[record_id] = {
            "cluster_id": cluster_id,
            "confidence": score
        }

singleton_id = cluster_id + 1

df_size = len(data.df)
data.df['original_id'] = y
data.df['confidence'] = np.array([None] * df_size)
data.df['cluster_id'] = np.array([None] * df_size)

for row_id in data.df.index.values.tolist():
    if row_id in cluster_membership:
        data.df.set_value(index=row_id, col='cluster_id', value=cluster_membership[row_id]["cluster_id"])
        data.df.set_value(index=row_id, col='confidence', value=cluster_membership[row_id]["confidence"])

data.df.sort_values(by=['original_id'], inplace=True)
data.df.to_csv(output_file)

duplicates = []
curr_cluster = []
curr_cluster_id = data.df.iloc[0]['cluster_id']
for row_id, row in data.df.iterrows():
    cluster_id = row['cluster_id']
    if cluster_id == curr_cluster_id:
        curr_cluster.append(row_id)
    else:
        if cluster_id is None:
            duplicates.append([row_id])
        else:
            duplicates.append(curr_cluster)
            curr_cluster_id = cluster_id
            curr_cluster = [row_id]

duplicates = sorted(duplicates, key=lambda arr: min(arr))
result = get_differences(true=data.true_duplicates['items'], predict=duplicates)
print("Accuracy: {}\n".format(get_accuracy(n_errors=result['number_of_errors'], n_records=df_size)))
print("Error number: {}\n".format(result['number_of_errors']))
