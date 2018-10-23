DROP SCHEMA IF EXISTS `LDC_CollectionDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_CollectionDB`;
GRANT ALL ON LDC_CollectionDB.* TO root@localhost;
ALTER DATABASE LDC_CollectionDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `LDC_CollectionDB`;

SOURCE WSHOME/ddl_ldc_collectiondb_mysql5innodb.sql;

insert into VendorConfig(PID, COLLECTING_FREQ, COLLECTOR_ENABLED, DOMAIN_CHECK_FIELD, DOMAIN_FIELD, MAX_ACTIVE_TASKS, VENDOR) values
(1, 15552000, 1, 'Technology_Name', 'Domain', 1, 'BUILTWITH'),
(2, 7776000, 0, '', '', 1, 'ALEXA'),
(3, 2592000, 0, '', '', 1, 'COMPETE'),
(4, 15552000, 0, '', '', 1, 'FEATURE'),
(5, 15552000, 0, '', '', 1, 'HPA_NEW'),
(6, 77762000, 0, '', '', 1, 'ORBINTELLIGENCEV2'),
(7, 2592000, 0, '', '', 1, 'SEMRUSH');
