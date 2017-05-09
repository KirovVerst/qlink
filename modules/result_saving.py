import os, json


class Logger(object):
    def __init__(self, folder_path, dataset_index=None):
        if dataset_index is None:
            self.folder_path = folder_path
        else:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path, exist_ok=True)
            self.folder_path = os.path.join(folder_path, str(dataset_index))

        if not os.path.exists(self.folder_path):
            os.mkdir(self.folder_path)

    def save_duplicates(self, duplicates, level=None):
        """
        
        :param duplicates: 
        :param level: 
        :return: 
        """

        if level is None:
            folder = self.folder_path
        else:
            folder = os.path.join(self.folder_path, str("_".join(list(map(str, level)))))

        if not os.path.exists(os.path.join(folder)):
            os.mkdir(folder)

        with open(os.path.join(folder, 'duplicates.json'), 'w') as f:
            json.dump(duplicates, f)

    def save_data(self, data, file_path=None):

        if file_path is None:
            file_path = os.path.join(self.folder_path, 'meta.json')

        with open(file_path, 'w') as f:
            json.dump(data, f)

    def save_errors(self, df, errors):
        """
        Error 1
        Predicted:
            1: first_name1 last_name1
            2: first_name2 last_name2
        True:
            1: first_name1 last_name1
        :param df: 
        :param errors: dict(items=[dict(true=[1,2], pred=[1,2,3])], 
                            level=0.7, 
                            extra_data=dict($id=dict(eval_levels=[float,], according=int)))
        :return: 
        
        """

        folder = os.path.join(self.folder_path, str("_".join(list(map(str, errors['level'])))))

        if not os.path.exists(os.path.join(folder)):
            os.mkdir(folder)

        with open(os.path.join(folder, "errors.json"), 'w') as fp:
            items = []
            for i, d in enumerate(errors['items']):
                item = dict(true=dict(), predicted=dict())

                true_duplicates = set(d['true'])
                pred_duplicates = set(d['predict'])

                dict_of_names = dict()

                for index in pred_duplicates.union(true_duplicates):
                    s = [df.iloc[index]['first_name'], df.iloc[index]['last_name'], df.iloc[index]['father_name']]
                    dict_of_names[index] = " ".join(s)

                for index in pred_duplicates:
                    item['predicted'][index] = dict(data=dict_of_names[index])
                    if index in errors["extra_data"]:
                        item['predicted'][index].update(errors["extra_data"][index])

                for index in true_duplicates:
                    item['true'][index] = dict(data=dict_of_names[index])
                    if index in errors["extra_data"]:
                        item['true'][index].update(errors["extra_data"][index])
                items.append(item)
            json.dump(dict(items=items), fp)
