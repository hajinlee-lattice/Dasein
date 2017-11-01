DROP SCHEMA IF EXISTS Data_MultiTenant;
CREATE SCHEMA IF NOT EXISTS Data_MultiTenant;
GRANT ALL ON Data_MultiTenant.* TO root@localhost;
USE `Data_MultiTenant`;

source WSHOME/ddl_data_multitenant_mysql5innodb.sql

