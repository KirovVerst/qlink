import os, json


def write_duplicates(folder, results):
    with open(os.path.join(folder, 'duplicates.txt'), 'w') as f:
        for group in results:
            f.write("{0}\n".format(group))


def write_meta_data(path, meta_data):
    with open(path, 'w') as f:
        json.dump(meta_data, f)


def write_errors(folder, data, errors):
    """
    Error 1
    Predicted:
        1: first_name1 last_name1
        2: first_name2 last_name2
    True:
        1: first_name1 last_name1
    :param folder: 
    :param data: 
    :param errors: [dict(true=[1,2], pred=[1,2,3])]
    :return: 
    
    """
    with open(os.path.join(folder, "errors.txt"), 'w') as f:
        for i, d in enumerate(errors):
            true_duplicates = set(d['true'])
            pred_duplicates = set(d['pred'])

            string_dict = dict()

            f.write("Error {0}:\n".format(i + 1))

            for index in pred_duplicates.union(true_duplicates):
                s = [data.iloc[index]['first_name'], data.iloc[index]['last_name'], data.iloc[index]['father']]
                string_dict[index] = " ".join(s)
            f.write("Predicted: {0}\n".format(str(d['pred'])))
            for index in pred_duplicates:
                f.write("\t{0}: {1}\n".format(index, string_dict[index]))
            f.write("True: {0}\n".format(str(d['true'])))
            for index in true_duplicates:
                f.write("\t{0}: {1}\n".format(index, string_dict[index]))
            f.write("\n")
