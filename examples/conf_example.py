import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATABASE = {
    'default': {
        "dialect": "sqlite",
        "db_name": "data/db/example.sqlite3",
        "query": "SELECT * FROM dataset",
        "user": "",
        "password": "",
        "host": "",  # example : localhost:3306
    },
    "mssql": {
        "db_name": "DatabaseName",
        "server_name": "ServerName",
        "driver": "{SQL Server}",
    }
}
