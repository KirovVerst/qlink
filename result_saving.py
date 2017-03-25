import os, datetime


def write_duplicates(folder, results):
    with open(os.path.join(folder, 'duplicates.txt'), 'w') as f:
        for group in results:
            f.write("{0}\n".format(group))


def write_meta_data(folder, dataset_size, error_number, start_time):
    with open(os.path.join(folder, "results.txt"), 'w') as f:
        f.write("Dataset size: {0}\n".format(dataset_size))
        f.write("Errors: {0}\n".format(error_number))
        end_time = datetime.datetime.now()
        d = end_time - start_time
        f.write("Time: {0}\n".format(d))
