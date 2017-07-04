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

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn204.csv' INTO TABLE `AccountMasterColumn`
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,BucketForSegment)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn205.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
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
(PID,IngestionName,Config,CronExpression,SchedularEnabled,NewJobRetryInterval,NewJobMaxRetry,IngestionType);

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

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceAttribute.csv' INTO TABLE `SourceAttribute`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceAttributeID,Arguments,Attribute,Source,Stage,Transformer);

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

UPDATE AccountMasterColumn
SET Groups = REPLACE(REPLACE(Groups, ',Segment', ''), 'Segment', '')
WHERE Groups LIKE '%Segment%'
AND (AMColumnID LIKE 'Bmbr30%' OR AMColumnID LIKE 'Feature%');



INSERT `DataCloudVersion` (Version, CreateDate, MajorVersion, Status, MetadataRefreshDate)
VALUES
  ('2.0.0', '2016-08-28', '2.0', 'APPROVED', NOW()),
  ('2.0.1', '2016-11-19', '2.0', 'APPROVED', NOW()),
  ('2.0.2', '2016-12-15', '2.0', 'APPROVED', NOW()),
  ('2.0.3', '2017-02-14', '2.0', 'APPROVED', NOW()),
  ('2.0.4', '2017-05-22', '2.0', 'APPROVED', NOW()),
  ('2.0.5', '2017-06-29', '2.0', 'APPROVED', NOW());

UPDATE `DataCloudVersion`
SET
  `DynamoTableSignature`        = '20161015',
  `DynamoTableSignature_Lookup` = '20161015'
WHERE `Version` = '2.0.0';

UPDATE `DataCloudVersion`
SET
  `DynamoTableSignature` = '20170126'
WHERE `Version` = '2.0.2';

UPDATE `DataCloudVersion`
SET
  `DynamoTableSignature`        = '20170301',
  `DynamoTableSignature_Lookup` = '20170604'
WHERE `Version` = '2.0.3';

UPDATE `DataCloudVersion`
SET
  `DynamoTableSignature`        = '20170604',
  `DynamoTableSignature_Lookup` = '20170604'
WHERE `Version` = '2.0.4';

UPDATE `DataCloudVersion`
SET
  `DynamoTableSignature`        = '20170629',
  `DynamoTableSignature_Lookup` = '20170629',
  `AMBucketedRedShiftTable`     = 'AccountMasterBucketed_2017_07_01_15_59_30_UTC',
  `SegmentStatsVersion`         = '2017-06-30_22-45-25_UTC'
WHERE `Version` = '2.0.5';

SET SQL_SAFE_UPDATES = 1;
