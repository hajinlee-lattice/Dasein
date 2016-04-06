DROP DATABASE PLS_MultiTenant;

CREATE DATABASE IF NOT EXISTS PLS_MultiTenant;

GRANT ALL on PLS_MultiTenant.* to root;

USE PLS_MultiTenant;

source WSHOME/le-db/ddl_pls_multitenant_mysql5innodb.sql

DROP DATABASE LDC_ManageDB;

CREATE DATABASE IF NOT EXISTS LDC_ManageDB;

GRANT ALL on LDC_ManageDB.* to root;

USE LDC_ManageDB;

source WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql



