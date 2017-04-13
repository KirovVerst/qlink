import os, csv, datetime
import pandas as pd
import dedupe

try:
    from conf import BASE_DIR
except:
    from conf_example import BASE_DIR

input_file = os.path.join(BASE_DIR, "data", "mockaroo", "ready", "3000", "data_3.csv")
output_file = os.path.join(BASE_DIR, "logs",
                           "output-{0}.csv".format(datetime.datetime.now().strftime("%d-%m-%y_%H_%M_%S")))
settings_file = os.path.join(BASE_DIR, "data", "mockaroo", "train", 'learned_settings')
training_file = os.path.join(BASE_DIR, "data", "mockaroo", "train", "training_dataset.json")


def read_data():
    df = pd.read_csv(input_file)
    df = df.sample(frac=1)
    y = df['original_id']
    del df['original_id']
    data = df.to_dict(orient='index')
    return data, y


print("importing data")
data_d, y = read_data()

fields = [
    {'field': 'first_name', 'type': 'String'},
    {'field': 'last_name', 'type': 'String'},
    {'field': 'father', 'type': 'String'},
    {'field': 'gender', 'type': 'Categorical', 'categories': ['M', 'F']}
]

deduper = dedupe.Dedupe(fields)

deduper.sample(data_d, 100)

if os.path.exists(training_file):
    print('reading labeled examples from ', training_file)
    with open(training_file, 'rb') as f:
        deduper.readTraining(f)

print('starting active labeling...')

dedupe.consoleLabel(deduper)

deduper.train()

with open(training_file, 'w') as tf:
    deduper.writeTraining(tf)

with open(settings_file, 'wb') as sf:
    deduper.writeSettings(sf)

threshold = deduper.threshold(data_d, recall_weight=1)

print("Clustering")

clustered_dupes = deduper.match(data_d, threshold)

cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    cluster_d = [data_d[c] for c in id_set]
    canonical_rep = dedupe.canonicalize(cluster_d)
    for record_id, score in zip(id_set, scores):
        cluster_membership[record_id] = {
            "cluster id": cluster_id,
            "canonical representation": canonical_rep,
            "confidence": score
        }

singleton_id = cluster_id + 1

with open(output_file, 'w') as f_output, open(input_file) as f_input:
    writer = csv.writer(f_output)
    reader = csv.reader(f_input)

    heading_row = next(reader)
    heading_row.insert(0, 'confidence_score')
    heading_row.insert(0, 'Cluster ID')
    writer.writerow(heading_row)

    for row in reader:
        row_id = int(row[0])
        if row_id in cluster_membership:
            cluster_id = cluster_membership[row_id]["cluster id"]
            row.insert(0, cluster_membership[row_id]['confidence'])
            row.insert(0, cluster_id)
        else:
            row.insert(0, None)
            row.insert(0, singleton_id)
            singleton_id += 1
        writer.writerow(row)

df = pd.read_csv(output_file)
df.sort(columns=['Cluster ID'], inplace=True)
df.to_csv(output_file, index=False)
