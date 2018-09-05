DROP SCHEMA IF EXISTS `DocumentDB`;
CREATE SCHEMA IF NOT EXISTS `DocumentDB`;
GRANT ALL ON DocumentDB.* TO root@localhost;
ALTER DATABASE DocumentDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `DocumentDB`;

SOURCE WSHOME/ddl_documentdb_mysql5innodb.sql;
