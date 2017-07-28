import os

import pandas as pd
import pyodbc

from examples.conf import DATABASE, BASE_DIR

DATA_PATH = os.path.join(BASE_DIR, 'data', 'spbu', 'data.csv')
COLUMN_NAMES = ["id", "FirstName", "SecondName", "MiddleName", "FirstNameEng", "SecondNameEng", "MiddleNameEng", "Sex",
                "BirthDate", "PlaceBirth", "Login", "Email", "Phone", "Country", "Region", "RegionCode", "Address",
                "DocumentName", "DocumentSeries", "DocumentNumber", "DocumentDate", "DocumentGivenOrganization",
                "EntranceYear", "Degree", "Faculty", "Speciality", "StudyForm", "StudyBasis"]


def select_data_from_db():
    con = pyodbc.connect('Trusted_Connection=yes',
                         driver=DATABASE['mssql']['driver'],
                         server=DATABASE['mssql']['server_name'],
                         database=DATABASE['mssql']['db_name'])

    cur = con.cursor()

    cur.execute(DATABASE['mssql']['query_create_view'])
    query_get_data = "select * from PersonalDataView"

    cur.execute(query_get_data)
    data = []
    for row in cur.fetchall():
        data.append(list(row))
    df = pd.DataFrame(data, columns=COLUMN_NAMES).set_index('id')
    df.to_csv(path_or_buf=DATA_PATH, encoding="utf-8")


def convert_birthday(birthday):
    s = birthday.split()
    return s[0] if len(s) > 1 else birthday


if __name__ == "__main__":
    dataframe = pd.read_csv(DATA_PATH, index_col='id')
