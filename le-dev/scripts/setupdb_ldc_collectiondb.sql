DROP SCHEMA IF EXISTS `LDC_CollectionDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_CollectionDB`;
GRANT ALL ON LDC_CollectionDB.* TO root@localhost;
ALTER DATABASE LDC_CollectionDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `LDC_CollectionDB`;

SOURCE WSHOME/ddl_ldc_collectiondb_mysql5innodb.sql;

insert into VendorConfig(PID, COLLECTING_FREQ, DOMAIN_CHECK_FIELD, DOMAIN_FIELD, MAX_ACTIVE_TASKS, GROUP_BY, SORT_BY,
CONSOLIDATION_PERIOD, LAST_CONSOLIDATED, VENDOR) values
(1, 15552000, 'Technology_Name', 'Domain', 1, 'Domain,Technology_Name', 'Last_Modification_Date', 31536000, null,
'BUILTWITH'),
(2, 7776000, 'OnlineSince', 'URL', 1, 'URL', 'LE_Last_Upload_Date', 63072000, null, 'ALEXA'),
(3, 2592000, '', '', 1, '', '', -1, null, 'COMPETE'),
(4, 15552000, '', '', 1, '', '', -1, null, 'FEATURE'),
(5, 15552000, '', '', 1, '', '', -1, null, 'HPA_NEW'),
(6, 77762000, 'orb_num', 'domain', 1, 'Domain', 'LE_Last_Upload_Date', 31536000, null, 'ORBINTELLIGENCEV2'),
(7, 2592000, 'Rank', 'Domain', 1, 'Domain', 'LE_Last_Upload_Date', 63072000, null, 'SEMRUSH');
