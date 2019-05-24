DROP SCHEMA IF EXISTS `LDC_ManageDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;
GRANT ALL ON LDC_ManageDB.* TO root@localhost;
ALTER DATABASE LDC_ManageDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `LDC_ManageDB`;

SOURCE WSHOME/ddl_ldc_managedb_mysql5innodb.sql;

# 2.0.6 & 2.0.14 version is needed for some testing purpose, do not remove them
# Besided above 2 versions, most recent 3 versions should be enough

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn206.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2014.csv' INTO TABLE `AccountMasterColumn`
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

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2017.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2018.csv' INTO TABLE `AccountMasterColumn`
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

INSERT `DecisionGraph` (GraphName, Vertices, StartingVertices, Edges, Description, JunctionGraphs, Entity, Retries)
VALUES
  ('Trilogy', 'DunsDomainBased,DomainBased,DunsBased', '0', '0:1|1:2', NULL, NULL, 'LatticeAccount', NULL),
  ('DragonClaw', 'DunsDomainBased,DomainBased,DunsBased,LocationToDuns', '0', '0:1,2,3|3:0,2', NULL, NULL, 'LatticeAccount', NULL),
  ('Halberd', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToDuns', '0', '0:1,2,3,4,5,6|6:0,5', NULL, NULL, 'LatticeAccount', NULL),
  ('Pokemon', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,LocationToDuns', '0', '0:1,2,3,4,5,6,7|7:0,5', 'Default decision graph in M23 and before', NULL, 'LatticeAccount', NULL),
  ('Pokemon2', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,LocationToDuns,DunsGuideValidate', '0', '0:1,2,3,4,5,6,7,8|8:0,5', 'First graph with DUNS redirect functionality', NULL, 'LatticeAccount', NULL),
  ('Gingerbread', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,CachedDunsValidate,LocationToDuns,DunsValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,5', 'Put DUNS validation functionality to a separate actor for better performance, no DUNS redirect, inherited from Pokemon', NULL, 'LatticeAccount', NULL),
  ('Honeycomb', 'DunsDomainBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,DunsBased,LocationToCachedDuns,CachedDunsGuideValidate,LocationToDuns,DunsGuideValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,5', 'Put DUNS validation functionality to a separate actor for better performance, have DUNS redirect, inherited from Pokemon2', NULL, 'LatticeAccount', NULL),
  ('IceCreamSandwich', 'DunsDomainBased,DunsBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,LocationToCachedDuns,CachedDunsValidate,LocationToDuns,DunsValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,1', 'Prioritize [DUNS] lookup over [Domain + Location] and [Domain] lookup, no DUNS redirect', NULL, 'LatticeAccount', NULL),
  ('JellyBean', 'DunsDomainBased,DunsBased,DomainCountryZipCodeBased,DomainCountryStateBased,DomainCountryBased,DomainBased,LocationToCachedDuns,CachedDunsGuideValidate,LocationToDuns,DunsGuideValidate', '0', '0:1,2,3,4,5,6,7,8,9|9:0,1', 'Prioritize [DUNS] lookup over [Domain + Location] and [Domain] lookup, have DUNS redirect', NULL, 'LatticeAccount', NULL),
  ('PetitFour', 'MatchPlanner,EntitySystemIdBased,FuzzyMatch,EntityDunsBased,EntityDomainCountryBased,EntityNameCountryBased,EntityIdAssociate,EntityIdResolve', '0', '0:1,2,3,4,5,6,7', 'Will retire after M28', 'FuzzyMatch:IceCreamSandwich', 'Account', 3),
  ('Cupcake', 'AccountMatchPlanner,EntitySystemIdBased,FuzzyMatch,EntityDomainCountryBased,EntityNameCountryBased,EntityDunsBased,EntityIdAssociate,EntityIdResolve', '0', '0:1,2,3,4,5,6,7', 'Default for Account entity', 'FuzzyMatch:IceCreamSandwich', 'Account', 3),
  ('Donut', 'ContactMatchPlanner,EntitySystemIdBased,AccountMatch,EntityEmailAIDBased,EntityNamePhoneAIDBased,EntityEmailBased,EntityNamePhoneBased,EntityIdAssociate', '0', '0:1,2,3,4,5,6,7', 'Default for Contact entity', 'AccountMatch:Cupcake', 'Contact', 3);


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
  ('2.0.6', '2017-09-01', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.14', '2018-09-17', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.15', '2018-10-27', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.16', '2018-12-10', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.17', '2019-02-25', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.18', '2019-05-03', '2.0', 'APPROVED', 'FULL', NOW(), '0');

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

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2018-12-06_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2018-12-06_00-00-00_UTC',
  `EnrichmentStatsVersion`   = '2018-12-06_00-00-00_UTC'
WHERE `Version` = '2.0.16';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2019-02-20_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2019-02-20_00-00-00_UTC',
  `EnrichmentStatsVersion`   = '2018-12-06_00-00-00_UTC',
  `DynamoTableSignature_Lookup` = '20190310'
WHERE `Version` = '2.0.17';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2019-04-05_00-00-00_UTC',
  `AccountLookupHdfsVersion` = '2019-04-04_00-00-00_UTC',
  `EnrichmentStatsVersion`   = '2019-04-05_00-00-00_UTC'
WHERE `Version` = '2.0.18';

SET SQL_SAFE_UPDATES = 1;
