DROP SCHEMA IF EXISTS PLS_MultiTenant;

CREATE SCHEMA IF NOT EXISTS PLS_MultiTenant;

GRANT ALL ON PLS_MultiTenant.* TO root;

USE `PLS_MultiTenant`;

source WSHOME/le-db/ddl_pls_multitenant_mysql5innodb.sql