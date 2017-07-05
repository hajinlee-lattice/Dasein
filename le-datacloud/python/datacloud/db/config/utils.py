from __future__ import absolute_import

import MySQLdb
import MySQLdb.cursors
import logging
from datacloud.common.cipher import decrypt

_CONFIG_DB = None
_logger = logging.getLogger(__name__)

def get_config_db():
    global _CONFIG_DB
    if _CONFIG_DB is None:
        pwd = decrypt(b'1AZy8-CiCvVE81AL66tHuqT6G5qwbD0zIOY1hBs45Po=')
        _CONFIG_DB = MySQLdb.connect(host="127.0.0.1", user="root", passwd=pwd,db="LDC_ConfigDB", cursorclass=MySQLdb.cursors.DictCursor)
        _logger.info("Setup a mysql connection to LDC_ConfigDB.")
    return _CONFIG_DB

def str_to_value(s):
    return ('\'%s\'' % s.replace("'", "''")) if s is not None else 'NULL'

def bool_to_value(s):
    return ('TRUE' if s else 'FALSE') if s is not None else 'NULL'