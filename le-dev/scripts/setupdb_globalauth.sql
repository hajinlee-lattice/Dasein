DROP SCHEMA IF EXISTS GlobalAuthentication;

CREATE SCHEMA IF NOT EXISTS GlobalAuthentication;

GRANT ALL ON GlobalAuthentication.* TO root@localhost;

USE `GlobalAuthentication`;

source WSHOME/le-db/ddl_globalauthentication_mysql5innodb.sql
