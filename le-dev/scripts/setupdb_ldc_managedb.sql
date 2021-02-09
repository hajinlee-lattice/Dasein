DROP SCHEMA IF EXISTS `LDC_ManageDB`;
CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;
GRANT ALL ON LDC_ManageDB.* TO root@localhost;
ALTER DATABASE LDC_ManageDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `LDC_ManageDB`;

SOURCE WSHOME/ddl_ldc_managedb_mysql5innodb.sql;

# 2.0.6 version is needed for some testing purpose, do not remove them
# Besided above version, most recent 3 versions should be enough

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn206.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,RefreshFrequency)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2024.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense,RefreshFrequency)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2025.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense,RefreshFrequency)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2026.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense,RefreshFrequency)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2027.csv' INTO TABLE `AccountMasterColumn`
CHARACTER SET UTF8
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,AMColumnID,DataCloudVersion,DisplayName,Description,JavaClass,Category,Subcategory,StatisticalType,DisplayDiscretizationStrategy,FundamentalType,ApprovedUsage,@var1,@var2,Groups,DecodeStrategy,@var3,EOLVersion,DataLicense,RefreshFrequency)
SET IsPremium = (@var1 = 'True' OR @var1 = 1), IsInternalEnrichment = (@var2 = 'True' OR @var2 = 1), IsEOL = (@var3 = 'True' OR @var3 = 1);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv' INTO TABLE `SourceColumn`
FIELDS TERMINATED BY '\t'
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
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceAttributeID,Arguments,Attribute,Source,Stage,Transformer,DataCloudVersion);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/CustomerSourceAttribute.csv' INTO TABLE `CustomerSourceAttribute`
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceAttributeID,Arguments,Attribute,Source,Stage,Transformer,DataCloudVersion);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/PrimeColumn.csv' INTO TABLE `PrimeColumn`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,Description,DisplayName,JavaClass,JsonPath,PrimeColumnId);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/DataBlockElement.csv' INTO TABLE `DataBlockElement`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,BlockID,Block,Level,Version,Element);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/DataBlockLevelMetadata.csv' INTO TABLE `DataBlockLevelMetadata`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,Block,Level,Description);

LOAD DATA INFILE 'WSHOME/le-dev/testartifacts/LDC_ManageDB/ContactMasterTpsColumn.csv' INTO TABLE `ContactMasterTpsColumn`
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID,ColumnName,JavaClass,IsDerived,MatchDestination);

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
  ('Donut', 'ContactMatchPlanner,EntitySystemIdBased,AccountMatch,EntityEmailAIDBased,EntityNamePhoneAIDBased,EntityEmailBased,EntityNamePhoneBased,EntityIdAssociate,EntityIdResolve', '0', '0:1,2,3,4,5,6,7,8', 'Default for Contact entity', 'AccountMatch:Cupcake', 'Contact', 3),
  ('WestWind', 'DunsToDuns,LocationToDuns,UrlToDuns', '0', '0:1|1:2', 'Default for DCP match', NULL, 'PrimeAccount', NULL),
  ('Heidegger', 'DunsToTps', '0', '', 'Default for TriPeopleSegment', NULL, 'TriPeopleSegment', NULL);

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

UPDATE AccountMasterColumn
SET Groups = REPLACE(REPLACE(Groups, ',Segment', ''), 'Segment', '')
WHERE Groups LIKE '%Segment%'
AND (AMColumnID LIKE 'Bmbr30%');

INSERT `DataCloudVersion` (Version, CreateDate, MajorVersion, Status, Mode, MetadataRefreshDate, RefreshVersion)
VALUES
  ('2.0.6', '2017-09-01', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.24', '2020-08-15', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.25', '2020-09-21', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.26', '2020-11-13', '2.0', 'APPROVED', 'FULL', NOW(), '0'),
  ('2.0.27', '2021-02-01', '2.0', 'APPROVED', 'FULL', NOW(), '0');

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
  `AccountMasterHdfsVersion` = '2020-08-15_18-19-27_UTC',
  `AccountLookupHdfsVersion` = '2020-08-15_18-19-27_UTC',
  `DunsGuideBookHdfsVersion` = '2020-08-15_18-19-27_UTC',
  `EnrichmentStatsVersion`   = '2020-08-15_18-19-27_UTC'
WHERE `Version` = '2.0.24';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2020-09-17_01-05-12_UTC',
  `AccountLookupHdfsVersion` = '2020-09-17_01-05-12_UTC',
  `DunsGuideBookHdfsVersion` = '2020-09-17_01-05-12_UTC',
  `EnrichmentStatsVersion`   = '2020-09-17_01-05-12_UTC'
WHERE `Version` = '2.0.25';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2020-11-11_05-28-19_UTC',
  `AccountLookupHdfsVersion` = '2020-11-07_04-09-53_UTC',
  `DunsGuideBookHdfsVersion` = '2020-11-07_04-09-53_UTC',
  `EnrichmentStatsVersion`   = '2020-11-11_17-31-47_UTC'
WHERE `Version` = '2.0.26';

UPDATE `DataCloudVersion`
SET
  `AccountMasterHdfsVersion` = '2021-01-29_08-03-39_UTC',
  `AccountLookupHdfsVersion` = '2021-01-24_18-07-55_UTC',
  `DunsGuideBookHdfsVersion` = '2021-01-24_19-27-09_UTC',
  `EnrichmentStatsVersion`   = '2021-02-01_08-38-31_UTC'
WHERE `Version` = '2.0.27';

UPDATE SourceColumn
SET Arguments = REPLACE(Arguments, 'Â', '')
WHERE SourceName = 'DnBCacheSeedRaw';

UPDATE SourceColumn
	SET Arguments = null WHERE Arguments = 'NULL';

UPDATE SourceColumn
	SET BaseSource = null WHERE BaseSource = 'NULL';

UPDATE SourceColumn
	SET Categories = null WHERE Categories = 'NULL';

UPDATE SourceColumn
	SET ColumnType = null WHERE ColumnType = 'NULL';

UPDATE SourceColumn
	SET GroupBy = null WHERE GroupBy = 'NULL';

UPDATE SourceColumn
	SET JoinBy = null WHERE JoinBy = 'NULL';

UPDATE SourceColumn
	SET Preparation = null WHERE Preparation = 'NULL';

UPDATE SourceAttribute
	SET Arguments = null WHERE Arguments = 'NULL';

UPDATE SourceAttribute
	SET DataCloudVersion = null WHERE DataCloudVersion = 'NULL';

UPDATE CustomerSourceAttribute
	SET Arguments = null WHERE Arguments = 'NULL';

UPDATE CustomerSourceAttribute
	SET DataCloudVersion = null WHERE DataCloudVersion = 'NULL';

UPDATE AccountMasterColumn
	SET ApprovedUsage = null WHERE ApprovedUsage = 'NULL';

UPDATE AccountMasterColumn
	SET DataLicense = null WHERE DataLicense = 'NULL';

UPDATE AccountMasterColumn
	SET DecodeStrategy = null WHERE DecodeStrategy = 'NULL';

UPDATE AccountMasterColumn
	SET Description = null WHERE Description = 'NULL';

UPDATE AccountMasterColumn
	SET DisplayDiscretizationStrategy = null WHERE DisplayDiscretizationStrategy = 'NULL';

UPDATE AccountMasterColumn
	SET EOLVersion = null WHERE EOLVersion = 'NULL';

UPDATE AccountMasterColumn
	SET FundamentalType = null WHERE FundamentalType = 'NULL';

UPDATE AccountMasterColumn
	SET RefreshFrequency = null WHERE RefreshFrequency = 'NULL';

UPDATE AccountMasterColumn
	SET StatisticalType = null WHERE StatisticalType = 'NULL';

UPDATE AccountMasterColumn
	SET Subcategory = null WHERE Subcategory = 'NULL';

SET SQL_SAFE_UPDATES = 1;
