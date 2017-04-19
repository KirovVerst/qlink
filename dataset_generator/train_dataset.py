import json, os
import pandas as pd

from record_metrics import levenshtein_edit_distance

try:
    from conf import BASE_DIR
except:
    from conf_example import BASE_DIR

JSON_TRAIN_DATA_PATH = os.path.join(BASE_DIR, 'data', 'mockaroo', 'ml', 'train.json')
CSV_TRAIN_DATA_PATH = os.path.join(BASE_DIR, 'data', 'mockaroo', 'ml', 'train.csv')
INIT_STR_FIELDS = ['first_name', 'last_name', 'father']
STR_FIELDS = ['first_name', 'first_name_len_1', 'first_name_len_2',
              'last_name', 'last_name_len_1', 'last_name_len_2',
              'father', 'father_len_1', 'father_len_2']
CATEGORY_FIELDS = ['gender']

FIELDS = STR_FIELDS + CATEGORY_FIELDS + ['match']


def get_read_raw_data():
    """
    
    :return: 
    {
        'distinct': [
            {
                '__class__': "tuple",
                '__value__': [
                    {
                      "first_name": "ulia",
                      "last_name": "Buurke",
                      "gender": "F",
                      "father": "Chrisopher"
                    },
                    {
                      "first_name": "RRalph",
                      "last_name": "Marshal",
                      "gender": "M",
                      "father": "Chrisopher"
                    }
                ]
            }
        ],
        'match': [
            {
                '__class__': "tuple",
                '__value__': [
                    {
                      "first_name": "ulia",
                      "last_name": "Buurke",
                      "gender": "F",
                      "father": "Chrisopher"
                    },
                    {
                      "first_name": "RRalph",
                      "last_name": "Marshal",
                      "gender": "M",
                      "father": "Chrisopher"
                    }
                ]
            }
        ]
    }
    """
    if os.path.exists(JSON_TRAIN_DATA_PATH):
        with open(JSON_TRAIN_DATA_PATH, 'r') as fp:
            data = json.load(fp)
            return data


def get_row(raw_data, match):
    """
    
    :param raw_data: {
                '__class__': "tuple",
                '__value__': [
                    {
                      "first_name": "ulia",
                      "last_name": "Buurke",
                      "gender": "F",
                      "father": "Chrisopher"
                    },
                    {
                      "first_name": "RRalph",
                      "last_name": "Marshal",
                      "gender": "M",
                      "father": "Chrisopher"
                    }
                ]
            }
    :param match: bool
    :return: []
    """
    raw_data = raw_data['__value__']
    result = []
    for field_name in INIT_STR_FIELDS:
        field_value_0 = raw_data[0][field_name]
        field_value_1 = raw_data[1][field_name]
        metric = levenshtein_edit_distance(field_value_0, field_value_1, normal='sum')
        result += [metric, len(field_value_0), len(field_value_1)]
    for field_name in CATEGORY_FIELDS:
        result.append(int(raw_data[0][field_name] == raw_data[1][field_name]))
    result.append(match)
    return result


if __name__ == '__main__':
    raw_data = get_read_raw_data()
    distinct_rows = list(map(lambda pair: get_row(pair, match=0), raw_data['distinct']))
    match_row = list(map(lambda pair: get_row(pair, match=1), raw_data['match']))
    rows = distinct_rows + match_row
    df = pd.DataFrame(rows, columns=FIELDS)
    df.to_csv(path_or_buf=CSV_TRAIN_DATA_PATH, index=False)
