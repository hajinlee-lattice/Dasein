"""
initialize the models package
"""
from models.engine import db_storage
from os import getenv

MYSQL_USER = getenv('MYSQL_USER')
MYSQL_PWD = getenv('MYSQL_PWD')
MYSQL_HOST = getenv('MYSQL_HOST')
MYSQL_DB = getenv('MYSQL_DB')

if MYSQL_USER and MYSQL_PWD and MYSQL_HOST and MYSQL_DB:
    storage = db_storage.DBStorage(
        MYSQL_USER=MYSQL_USER,
        MYSQL_PWD=MYSQL_PWD,
        MYSQL_HOST=MYSQL_HOST,
        MYSQL_DB=MYSQL_DB
    )
