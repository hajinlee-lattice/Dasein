DROP SCHEMA IF EXISTS Data_MultiTenant;
CREATE SCHEMA IF NOT EXISTS Data_MultiTenant CHARACTER SET utf8;
GRANT ALL ON Data_MultiTenant.* TO root@localhost;
USE `Data_MultiTenant`;

source WSHOME/le-datadb/ddl_data_multitenant_mysql5innodb.sql

