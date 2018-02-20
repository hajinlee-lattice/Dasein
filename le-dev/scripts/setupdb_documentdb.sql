DROP SCHEMA IF EXISTS `DocumentDB`;
CREATE SCHEMA IF NOT EXISTS `DocumentDB`;
GRANT ALL ON DocumentDB.* TO root@localhost;
ALTER DATABASE DocumentDB CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `DocumentDB`;

SOURCE WSHOME/ddl_documentdb_mysql5innodb.sql;
