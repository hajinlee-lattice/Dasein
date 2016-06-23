DROP SCHEMA IF EXISTS GlobalAuthentication;

CREATE SCHEMA IF NOT EXISTS GlobalAuthentication;

GRANT ALL ON GlobalAuthentication.* TO root;

USE `GlobalAuthentication`;

source WSHOME/le-db/ddl_globalauthentication_mysql5innodb.sql

source WSHOME/le-security/src/test/schema/schema_mysql.sql
