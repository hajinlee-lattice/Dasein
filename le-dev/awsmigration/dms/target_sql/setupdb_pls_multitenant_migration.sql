DROP SCHEMA IF EXISTS PLS_MultiTenant;

CREATE SCHEMA IF NOT EXISTS PLS_MultiTenant;

GRANT ALL ON PLS_MultiTenant.* TO root@localhost;

USE `PLS_MultiTenant`;

source ddl_pls_multitenant_mysql5innodb.sql
