DROP SCHEMA IF EXISTS `LDC_ManageDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;
GRANT ALL ON LDC_ManageDB.* TO root@localhost;
ALTER DATABASE LDC_ManageDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `LDC_ManageDB`;

SOURCE WSHOME/ddl_ldc_managedb_mysql5innodb.sql;

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn206.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn208.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn209.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2010.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2011.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2012.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2013.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2014.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2015.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2016.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

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
(PID,IngestionName,Config,CronExpression,@var1,NewJobRetryInterval,NewJobMaxRetry,IngestionType)
SET SchedularEnabled = (@var1 = 'True' OR @var1 = 1);

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
(SourceAttributeID,Arguments,Attribute,Source,Stage,Transformer,DataCloudVersion);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/CustomerSourceAttribute.csv' INTO TABLE `CustomerSourceAttribute`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceAttributeID,Arguments,Attribute,Source,Stage,Transformer,DataCloudVersion);

INSERT `DecisionGraph` (GraphName, Vertices, StartingVertices, Edges, Description)
VALUES
  ('Trilogy', 'DunsDomainBased,DomainBased,DunsBased', '0', '0:1|1:2', NULL),
  ('DragonClaw', 'DunsDomainBased,DomainBased,DunsBased,LocationToDuns', '0', '0:1,2,3|3:0,2', NULL),
  ('Halberd', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToDuns', '0', '0:1,2,3,4,5,6|6:0,5', NULL),
  ('Pokemon', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,LocationToDuns', '0', '0:1,2,3,4,5,6,7|7:0,5', 'Default decision graph in M23 and before'),
  ('Pokemon2', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,LocationToDuns,DunsGuideValidate', '0', '0:1,2,3,4,5,6,7,8|8:0,5', 'First graph with DUNS redirect functionality'),
  ('Gingerbread', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,CachedDunsValidate,LocationToDuns,DunsValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,5', 'Put DUNS validation functionality to a separate actor for better performance, no DUNS redirect, inherited from Pokemon'),
  ('Honeycomb', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,CachedDunsGuideValidate,LocationToDuns,DunsGuideValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,5', 'Put DUNS validation functionality to a separate actor for better performance, have DUNS redirect, inherited from Pokemon2'),
  ('IceCreamSandwich', 'DunsDomainBased,DunsBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,LocationToCachedDuns,CachedDunsValidate,LocationToDuns,DunsValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,1', 'Prioritize [DUNS] lookup over [Domain + Location] and [Domain] lookup, no DUNS redirect'),
  ('JellyBean', 'DunsDomainBased,DunsBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,LocationToCachedDuns,CachedDunsGuideValidate,LocationToDuns,DunsGuideValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,1', 'Prioritize [DUNS] lookup over [Domain + Location] and [Domain] lookup, have DUNS redirect');


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
AND (AMColumnID LIKE 'Bmbr30%');

INSERT `DataCloudVersion` (Version, CreateDate, MajorVersion, Status, Mode, MetadataRefreshDate, RefreshVersion)
VALUES
  ('2.0.0', '2016-08-28', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.1', '2016-11-19', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.2', '2016-12-15', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.3', '2017-02-14', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.4', '2017-05-22', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.5', '2017-06-29', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.6', '2017-09-01', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.7', '2017-10-09', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.8', '2017-11-17', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.9', '2018-01-28', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.10', '2018-03-02', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.11', '2018-04-24', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.12', '2018-05-15', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.13', '2018-07-24', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.14', '2018-09-17', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.15', '2018-10-27', '2.0', 'APPROVED', 'FULL', NOW(), '0');

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2016-10-15_14-37-09_UTC',
  `AccountLookupHdfsVersion` = '2016-10-10_17-40-35_UTC',
  `DynamoTableSignature`        = '20161015',
  `DynamoTableSignature_Lookup` = '20161015'
WHERE `Version` = '2.0.0';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2016-11-19_20-32-21_UTC',
  `AccountLookupHdfsVersion` = '2016-11-19_05-33-46_UTC'
WHERE `Version` = '2.0.1';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2017-01-24_23-14-40_UTC',
  `AccountLookupHdfsVersion` = '2017-01-04_04-49-12_UTC',
  `DynamoTableSignature` = '20170126'
WHERE `Version` = '2.0.2';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2017-02-14_07-28-18_UTC',
  `AccountLookupHdfsVersion` = '2017-02-14_17-20-41_UTC',
  `DynamoTableSignature`        = '20170301',
  `DynamoTableSignature_Lookup` = '20170604'
WHERE `Version` = '2.0.3';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2017-05-18_12-54-37_UTC',
  `AccountLookupHdfsVersion` = '2017-05-17_17-30-29_UTC',
  `DynamoTableSignature`        = '20170604',
  `DynamoTableSignature_Lookup` = '20170604'
WHERE `Version` = '2.0.4';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2017-06-30_01-18-36_UTC',
  `AccountLookupHdfsVersion` = '2017-06-03_00-14-03_UTC',
  `DynamoTableSignature`        = '20170629',
  `DynamoTableSignature_Lookup` = '20170629',
  `EnrichmentStatsVersion`      = '2017-07-22_04-52-07_UTC'
WHERE `Version` = '2.0.5';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2017-08-30_11-20-17_UTC',
  `AccountLookupHdfsVersion` = '2017-08-30_06-53-34_UTC',
  `DynamoTableSignature`        = '20170830',
  `DynamoTableSignature_Lookup` = '20170830',
  `EnrichmentStatsVersion`      = '2017-08-30_16-45-58_UTC'
WHERE `Version` = '2.0.6';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2017-10-08_19-08-20_UTC',
  `AccountLookupHdfsVersion` = '2017-10-08_20-01-11_UTC',
  `EnrichmentStatsVersion`      = '2017-08-30_16-45-58_UTC'
WHERE `Version` = '2.0.7';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2017-11-16_05-41-36_UTC',
  `AccountLookupHdfsVersion` = '2017-11-16_05-41-36_UTC',
  `EnrichmentStatsVersion`      = '2017-08-30_16-45-58_UTC'
WHERE `Version` = '2.0.8';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-01-10_05-41-36_UTC',
  `AccountLookupHdfsVersion` = '2018-01-10_05-41-36_UTC',
  `EnrichmentStatsVersion`      = '2017-08-30_16-45-58_UTC'
WHERE `Version` = '2.0.9';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-02-28_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2018-02-28_00-00-00_UTC',
  `EnrichmentStatsVersion`      = '2017-08-30_16-45-58_UTC'
WHERE `Version` = '2.0.10';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-04-20_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2018-04-20_00-00-00_UTC',
  `DynamoTableSignature`        = '20180420',
  `DynamoTableSignature_Lookup` = '20180420',
  `EnrichmentStatsVersion`      = '2017-08-30_16-45-58_UTC'
WHERE `Version` = '2.0.11';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-05-23_05-41-11_UTC',
  `AccountLookupHdfsVersion` = '2018-05-10_00-00-00_UTC',
  `EnrichmentStatsVersion`   = '2018-05-10_00-00-00_UTC',
  `DynamoTableSignature` = '20180523'
WHERE `Version` = '2.0.12';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-07-20_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2018-07-18_00-00-00_UTC',
  `EnrichmentStatsVersion`   = '2018-05-10_00-00-00_UTC'
WHERE `Version` = '2.0.13';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-09-15_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2018-09-15_00-00-00_UTC',
  `EnrichmentStatsVersion`   = '2018-09-15_00-00-00_UTC',
  `DynamoTableSignature_DunsGuideBook` = '20180918'
WHERE `Version` = '2.0.14';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-10-23_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2018-10-23_00-00-00_UTC',
  `EnrichmentStatsVersion`   = '2018-10-23_00-00-00_UTC'
WHERE `Version` = '2.0.15';

SET SQL_SAFE_UPDATES = 1;
