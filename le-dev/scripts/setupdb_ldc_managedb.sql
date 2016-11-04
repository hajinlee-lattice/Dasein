DROP SCHEMA IF EXISTS `LDC_ManageDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;
GRANT ALL ON LDC_ManageDB.* TO root@localhost;
USE `LDC_ManageDB`;

source WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql;

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv' INTO TABLE `SourceColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceColumnID,SourceName,ColumnName,ColumnType,BaseSource,Preparation,Priority,GroupBy,JoinBy,Calculation,Arguments,Groups,CharAttrId,Categories);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/ExternalColumn.csv' INTO TABLE `ExternalColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,ExternalColumnID,DefaultColumnName,TablePartition,Description,DataType,DisplayName,Category,SubCategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,MatchDestination,Tags);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn.csv' INTO TABLE `AccountMasterColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,IsPremium,Groups,DecodeStrategy);


LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/Publication.csv' INTO TABLE `Publication`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

INSERT `DataCloudVersion` (Version, CreateDate, MajorVersion, AccountMasterHdfsVersion, AccountLookupHdfsVersion, DynamoTableSignature, Status)
VALUES
  ('2.0.0', '2016-08-28', '2.0', '2016-10-15_14-37-09_UTC', '2016-10-10_17-40-35_UTC', '20161015', 'APPROVED'),
  ('2.0.1', '2016-10-28', '2.0', '2016-10-28_17-32-32_UTC', '2016-10-28_14-45-38_UTC', '', 'NEW');

INSERT `DecisionGraph` (GraphName, Vertices, StartingVertices, Edges)
VALUES
  ('Trilogy', 'DunsDomainBased,DomainBased,DunsBased', 'DunsDomainBased', '0:1|1:2'),
  ('DragonClaw', 'DunsDomainBased,DomainBased,DunsBased,LocationToDuns', 'DunsDomainBased', '0:1,2,3|3:0');


LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/CountryCode.csv' INTO TABLE `CountryCode`
CHARACTER SET UTF8
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

UPDATE AccountMasterColumn
SET StatisticalType = NULL
WHERE StatisticalType = '';

UPDATE AccountMasterColumn
SET FundamentalType = NULL
WHERE FundamentalType = '';

UPDATE SourceColumn
SET Arguments = REPLACE(Arguments, 'Â', '')
WHERE SourceName = 'DnBCacheSeedRaw';

SET SQL_SAFE_UPDATES = 1;
