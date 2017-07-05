# -*- coding: UTF-8 -*-
from __future__ import absolute_import

import MySQLdb
import MySQLdb.cursors
import logging
import pymssql
from datacloud.common.cipher import decrypt

_CONFIG_DB = None
_SQL231_MANAGE_DB = None
_logger = logging.getLogger(__name__)

def get_config_db():
    global _CONFIG_DB
    if _CONFIG_DB is None:
        pwd = decrypt(b'1AZy8-CiCvVE81AL66tHuqT6G5qwbD0zIOY1hBs45Po=')
        _CONFIG_DB = MySQLdb.connect(host="127.0.0.1", user="root", passwd=pwd, db="LDC_ConfigDB", charset='utf8', cursorclass=MySQLdb.cursors.DictCursor)
        _logger.info("Setup a MySQL connection to LDC_ConfigDB.")
    return _CONFIG_DB

def get_sql231_manage_db():
    global _SQL231_MANAGE_DB
    if _SQL231_MANAGE_DB is None:
        pwd = decrypt(b'21YUsNp7KQRmkPrp0Lbq2zS4O5ycIJgSQGRbSeXUTRk=')
        _SQL231_MANAGE_DB = pymssql.connect('bodcprodvsql231.prod.lattice.local', 'DLTransfer', pwd, "LDC_ManageDB", as_dict=True)
        _logger.info("Setup a SQL Server connection to LDC_ManageDB on SQL231.")
    return _SQL231_MANAGE_DB

def str_to_value(s):
    return ('\'%s\'' % s.replace("'", "''")) if s is not None else 'NULL'

def bool_to_value(s):
    return ('TRUE' if s else 'FALSE') if s is not None else 'NULL'

def utf8_to_latin1(s):
    if s is None:
        return None
    try:
        utf_s = s
        utf_s = utf_s.replace(u'\u2013', '-')
        utf_s = utf_s.replace(u'\u2014', '-')
        utf_s = utf_s.replace(u'\u2019', "'")
        utf_s = utf_s.replace(u'\u2310', "")
        return utf_s
    except Exception:
        _logger.error("Failed to convert string %s to latin-1", s)
        raise