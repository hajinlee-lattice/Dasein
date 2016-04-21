DROP SCHEMA IF EXISTS `LDC_ManageDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;
GRANT ALL ON LDC_ManageDB.* TO root;
USE `LDC_ManageDB`;

source WSHOME/le-propdata/ddl_ldc_managedb_mysql5innodb.sql;

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/SourceColumn.csv' INTO TABLE `SourceColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceColumnID, SourceName, ColumnName, ColumnType, BaseSource, Preparation, Priority, GroupBy, Calculation, Arguments, Groups);

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/ExternalColumn.csv' INTO TABLE `ExternalColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID, ExternalColumnID, DefaultColumnName, Description, DataType, DisplayName, Category, StatisticalType, DisplayDiscretizationStrategy, FundamentalType, ApprovedUsage, Tags);

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/ColumnMapping.csv' INTO TABLE `ColumnMapping`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID, ExternalColumnID, SourceName, SourceColumn, Priority);

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/Publication.csv' INTO TABLE `Publication`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SET SQL_SAFE_UPDATES = 0;

UPDATE ExternalColumn
SET StatisticalType = NULL
WHERE StatisticalType = '';

UPDATE ExternalColumn
SET FundamentalType = NULL
WHERE FundamentalType = '';

UPDATE ExternalColumn
SET DataType = NULL
WHERE DataType = '';

SET SQL_SAFE_UPDATES = 1;
