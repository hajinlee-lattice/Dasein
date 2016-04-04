CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;

USE `LDC_ManageDB`;

drop table if exists `ArchiveProgress`;

drop table if exists `ColumnMapping`;

drop table if exists `ExternalColumn`;

drop table if exists `MatchCommand`;

drop table if exists `MatchOperation`;

drop table if exists `RefreshProgress`;

drop table if exists `SourceColumn`;

create table `ArchiveProgress` (
    `ProgressID` bigint not null auto_increment unique,
    `CreateTime` datetime,
    `CreatedBy` varchar(255),
    `EndDate` datetime,
    `ErrorMessage` varchar(255),
    `LatestStatusUpdate` datetime,
    `NumRetries` integer,
    `RootOperationUID` varchar(255) not null unique,
    `RowsDownloadedToHDFS` bigint,
    `RowsUploadedToSQL` bigint,
    `SourceName` varchar(255) not null,
    `StartDate` datetime,
    `Status` varchar(255),
    `StatusBeforeFailed` varchar(255),
    primary key (`ProgressID`)
) ENGINE=InnoDB;

create table `ColumnMapping` (
    `PID` bigint not null auto_increment unique,
    `Priority` integer,
    `SourceColumn` varchar(100),
    `SourceName` varchar(100) not null,
    ExternalColumnID varchar(100) not null,
    primary key (`PID`)
) ENGINE=InnoDB;

create table `ExternalColumn` (
    `ExternalColumnID` varchar(100) not null,
    `ApprovedUsage` varchar(255),
    `Category` varchar(50),
    `DataType` varchar(50) not null,
    `DefaultColumnName` varchar(100) not null,
    `Description` varchar(1000) not null,
    `DisplayDiscretizationStrategy` varchar(1000),
    `DisplayName` varchar(255),
    `FundamentalType` varchar(50),
    `PID` bigint not null unique,
    `StatisticalType` varchar(50),
    `Tags` varchar(500),
    primary key (`ExternalColumnID`)
) ENGINE=InnoDB;

create table `MatchCommand` (
    `PID` bigint not null auto_increment,
    `ApplicationId` varchar(255),
    `ColumnSelection` varchar(255),
    `CreateTime` datetime,
    `Customer` varchar(255),
    `ErrorMessage` varchar(255),
    `LatestStatusUpdate` datetime,
    `MatchStatus` varchar(20) not null,
    `NumRetries` integer,
    `NumRows` integer not null,
    `Progress` float,
    `RootOperationUID` varchar(100) not null unique,
    `StatusBeforeFailed` varchar(20),
    primary key (`PID`)
) ENGINE=InnoDB;

create table `MatchOperation` (
    `PID` bigint not null auto_increment,
    `ApplicationId` varchar(255),
    `ApplicationState` varchar(20) not null,
    `BlockOperationUID` varchar(100) not null,
    `CreateTime` datetime,
    `ErrorMessage` varchar(255),
    `LatestStatusUpdate` datetime,
    `NumRetries` integer,
    `NumRows` integer not null,
    `Progress` float,
    `StateBeforeFailed` varchar(20),
    RootOperationUID bigint not null,
    primary key (`PID`)
) ENGINE=InnoDB;

create table `RefreshProgress` (
    `ProgressID` bigint not null auto_increment unique,
    `BaseSourceVersion` varchar(255),
    `CreateTime` datetime,
    `CreatedBy` varchar(255),
    `ErrorMessage` varchar(255),
    `LatestStatusUpdate` datetime,
    `NumRetries` integer,
    `PivotDate` datetime,
    `RootOperationUID` varchar(255) not null unique,
    `RowsGeneratedInHDFS` bigint,
    `RowsUploadedToSQL` bigint,
    `SourceName` varchar(255) not null,
    `Status` varchar(255),
    `StatusBeforeFailed` varchar(255),
    primary key (`ProgressID`)
) ENGINE=InnoDB;

create table `SourceColumn` (
    `SourceColumnID` bigint not null auto_increment unique,
    `Arguments` varchar(1000),
    `BaseSource` varchar(100),
    `Calculation` varchar(50) not null,
    `ColumnName` varchar(100) not null,
    `ColumnType` varchar(50) not null,
    `GroupBy` varchar(100),
    `Groups` varchar(255) not null,
    `Preparation` varchar(1000),
    `Priority` integer not null,
    `SourceName` varchar(100) not null,
    primary key (`SourceColumnID`)
) ENGINE=InnoDB;

alter table `ColumnMapping`
    add index FK9166C0784570DE77 (ExternalColumnID),
    add constraint FK9166C0784570DE77
    foreign key (ExternalColumnID)
    references `ExternalColumn` (`ExternalColumnID`)
    on delete cascade;

alter table `MatchOperation`
    add index FK78AE1542D791BF6B (RootOperationUID),
    add constraint FK78AE1542D791BF6B
    foreign key (RootOperationUID)
    references `MatchCommand` (`PID`)
    on delete cascade;


LOAD DATA INFILE '/home/build/Projects/ledp/le-propdata/src/test/resources/sql/SourceColumn.txt' INTO TABLE `SourceColumn`
FIELDS TERMINATED BY '\t'
ENCLOSED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(SourceColumnID, SourceName, ColumnName, ColumnType, BaseSource, Preparation, Priority, GroupBy, Calculation, Arguments, Groups);

LOAD DATA INFILE '/home/build/Projects/ledp/le-propdata/src/test/resources/sql/ExternalColumn.txt' INTO TABLE `ExternalColumn`
FIELDS TERMINATED BY '\t'
ENCLOSED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID, ExternalColumnID, DefaultColumnName, Description, DataType, DisplayName, Category, StatisticalType, DisplayDiscretizationStrategy, FundamentalType, ApprovedUsage, Tags);

LOAD DATA INFILE '/home/build/Projects/ledp/le-propdata/src/test/resources/sql/ColumnMapping.txt' INTO TABLE `ColumnMapping`
FIELDS TERMINATED BY '\t'
ENCLOSED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(PID, ExternalColumnID, SourceName, SourceColumn, Priority);

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
