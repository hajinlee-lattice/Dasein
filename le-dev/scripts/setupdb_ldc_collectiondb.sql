DROP SCHEMA IF EXISTS `LDC_CollectionDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_CollectionDB`;
GRANT ALL ON LDC_CollectionDB.* TO root@localhost;
ALTER DATABASE LDC_CollectionDB CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `LDC_CollectionDB`;

SOURCE WSHOME/ddl_ldc_collectiondb_mysql5innodb.sql;
