DROP SCHEMA IF EXISTS `LDC_ManageDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;
GRANT ALL ON LDC_ManageDB.* TO root@localhost;
USE `LDC_ManageDB`;

source WSHOME/le-propdata/ddl_ldc_managedb_mysql5innodb.sql;

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/SourceColumn.csv' INTO TABLE `SourceColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceColumnID,SourceName,ColumnName,ColumnType,BaseSource,Preparation,Priority,GroupBy,JoinBy,Calculation,Arguments,Groups);

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/ExternalColumn.csv' INTO TABLE `ExternalColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,ExternalColumnID,DefaultColumnName,TablePartition,Description,DataType,DisplayName,Category,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,MatchDestination,Tags);

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/AccountMasterColumn.csv' INTO TABLE `AccountMasterColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,IsPremium,Groups);


LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/Publication.csv' INTO TABLE `Publication`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA INFILE 'WSHOME/le-propdata/src/test/resources/sql/Ingestion.csv' INTO TABLE `Ingestion`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,IngestionName,Source,CronExpression,SchedularEnabled,NewJobRetryInterval,NewJobMaxRetry,IngestionType,IngestionCriteria);

INSERT `DataCloudVersion` (Version, CreateDate, MajorVersion, HdfsVersion, Status)
VALUES
  ('2.0.0', '2016-07-01', '2.0', '2016-07-01_00-00-00_UTC', 'APPROVED'),
  ('2.0.1', '2016-08-01', '2.0', '2016-08-01_00-00-00_UTC', 'NEW');

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

UPDATE LDC_ManageDB.SourceColumn
SET Arguments = REPLACE(Arguments, 'Â', '')
WHERE SourceName = 'DnBCacheSeedRaw';

SET SQL_SAFE_UPDATES = 1;
