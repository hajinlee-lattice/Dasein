DROP SCHEMA IF EXISTS `LDC_ManageDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;
GRANT ALL ON LDC_ManageDB.* TO root@localhost;
USE `LDC_ManageDB`;

SOURCE WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql;

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn200.csv' INTO TABLE `AccountMasterColumn`
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,BucketForSegment)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn201.csv' INTO TABLE `AccountMasterColumn`
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,BucketForSegment)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn202.csv' INTO TABLE `AccountMasterColumn`
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,BucketForSegment)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn203.csv' INTO TABLE `AccountMasterColumn`
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,BucketForSegment)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1);

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

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/Publication.csv' INTO TABLE `Publication`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/Ingestion.csv' INTO TABLE `Ingestion`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,IngestionName,Source,CronExpression,SchedularEnabled,NewJobRetryInterval,NewJobMaxRetry,IngestionType,IngestionCriteria);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalDimension.csv' INTO TABLE `CategoricalDimension`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID, Dimension, RootAttrId, Source);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv' INTO TABLE `CategoricalAttribute`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID, AttrName, AttrValue, ParentID);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterFact.csv' INTO TABLE `AccountMasterFact`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,Category,EncodedCube,Industry,Location,NumEmpRange,NumLocRange,RevRange);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/LatticeIdStrategy.csv' INTO TABLE `LatticeIdStrategy`
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

INSERT `DataCloudVersion` (Version, CreateDate, MajorVersion, AccountMasterHdfsVersion, AccountLookupHdfsVersion, DynamoTableSignature, DynamoTableSignature_Lookup, Status)
VALUES
  ('2.0.0', '2016-08-28', '2.0', '2016-10-15_14-37-09_UTC', '2016-10-10_17-40-35_UTC', '20161015', '20161015', 'APPROVED'),
  ('2.0.1', '2016-11-19', '2.0', '2016-11-19_20-32-21_UTC', '2016-11-19_05-33-46_UTC', '', '', 'APPROVED'),
  ('2.0.2', '2016-12-15', '2.0', '2017-01-24_23-14-40_UTC', '2017-01-04_04-49-12_UTC', '20170126', '', 'APPROVED'),
  ('2.0.3', '2017-02-14', '2.0', '2017-02-14_07-28-18_UTC', '2017-02-14_17-20-41_UTC', '20170301', '20170301', 'APPROVED');

INSERT `DecisionGraph` (GraphName, Vertices, StartingVertices, Edges)
VALUES
  ('Trilogy', 'DunsDomainBased,DomainBased,DunsBased', '0', '0:1|1:2'),
  ('DragonClaw', 'DunsDomainBased,DomainBased,DunsBased,LocationToDuns', '0', '0:1,2,3|3:0,2'),
  ('Halberd', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToDuns', '0', '0:1,2,3,4,5,6|6:0,5');


LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/CountryCode.csv' INTO TABLE `CountryCode`
CHARACTER SET UTF8
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

SET SQL_SAFE_UPDATES = 0;

UPDATE ExternalColumn
SET StatisticalType = NULL
WHERE StatisticalType = '' OR StatisticalType = 'NULL';

UPDATE ExternalColumn
SET FundamentalType = NULL
WHERE FundamentalType = '' OR FundamentalType = 'NULL';

UPDATE ExternalColumn
SET DataType = NULL
WHERE DataType = '' OR DataType = 'NULL';

UPDATE AccountMasterColumn
SET StatisticalType = NULL
WHERE StatisticalType = '' OR StatisticalType = 'NULL';

UPDATE AccountMasterColumn
SET FundamentalType = NULL
WHERE FundamentalType = '' OR FundamentalType = 'NULL';

UPDATE AccountMasterColumn
SET DecodeStrategy = NULL
WHERE DecodeStrategy = '' OR DecodeStrategy = 'NULL';

UPDATE AccountMasterColumn
SET DisplayDiscretizationStrategy = NULL
WHERE DisplayDiscretizationStrategy = '' OR DisplayDiscretizationStrategy = 'NULL';

UPDATE SourceColumn
SET Arguments = REPLACE(Arguments, 'Â', '')
WHERE SourceName = 'DnBCacheSeedRaw';

SET SQL_SAFE_UPDATES = 1;
