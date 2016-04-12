DROP DATABASE LDC_ManageDB;

CREATE DATABASE IF NOT EXISTS LDC_ManageDB;

GRANT ALL ON LDC_ManageDB.* TO root;

USE LDC_ManageDB;

source WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql