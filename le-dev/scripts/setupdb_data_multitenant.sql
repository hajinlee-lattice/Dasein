DROP SCHEMA IF EXISTS Data_MultiTenant;
CREATE SCHEMA IF NOT EXISTS Data_MultiTenant;
GRANT ALL ON Data_MultiTenant.* TO root@localhost;
ALTER DATABASE Data_MultiTenant CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `Data_MultiTenant`;

source WSHOME/ddl_data_multitenant_mysql5innodb.sql

