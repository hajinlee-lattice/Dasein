CREATE SCHEMA IF NOT EXISTS `LDC_ManageDB`;

USE `LDC_ManageDB`;

DROP TABLE IF EXISTS `ArchiveProgress`;

DROP TABLE IF EXISTS `ColumnMapping`;

DROP TABLE IF EXISTS `ExternalColumn`;

DROP TABLE IF EXISTS `RefreshProgress`;

DROP TABLE IF EXISTS `SourceColumn`;

CREATE TABLE `ArchiveProgress` (
  `ProgressID`           BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
  `CreateTime`           DATETIME,
  `CreatedBy`            VARCHAR(255),
  `EndDate`              DATETIME,
  `ErrorMessage`         VARCHAR(255),
  `LatestStatusUpdate`   DATETIME,
  `NumRetries`           INTEGER,
  `RootOperationUID`     VARCHAR(255) NOT NULL UNIQUE,
  `RowsDownloadedToHDFS` BIGINT,
  `RowsUploadedToSQL`    BIGINT,
  `SourceName`           VARCHAR(255) NOT NULL,
  `StartDate`            DATETIME,
  `Status`               VARCHAR(255),
  `StatusBeforeFailed`   VARCHAR(255),
  PRIMARY KEY (`ProgressID`),
  UNIQUE (`RootOperationUID`)
)
  ENGINE = InnoDB;

CREATE TABLE `ColumnMapping` (
  `PID`            BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
  `Priority`       INTEGER,
  `SourceColumn`   VARCHAR(100),
  `SourceName`     VARCHAR(100) NOT NULL,
  ExternalColumnID VARCHAR(100) NOT NULL,
  PRIMARY KEY (`PID`)
)
  ENGINE = InnoDB;

CREATE TABLE `ExternalColumn` (
  `ExternalColumnID`              VARCHAR(100)  NOT NULL,
  `ApprovedUsage`                 VARCHAR(255),
  `Category`                      VARCHAR(50),
  `DataType`                      VARCHAR(50)   NOT NULL,
  `DefaultColumnName`             VARCHAR(100)  NOT NULL,
  `Description`                   VARCHAR(1000) NOT NULL,
  `DisplayDiscretizationStrategy` VARCHAR(1000),
  `DisplayName`                   VARCHAR(255),
  `FundamentalType`               VARCHAR(50),
  `PID`                           BIGINT        NOT NULL UNIQUE,
  `StatisticalType`               VARCHAR(50),
  `Tags`                          VARCHAR(500),
  PRIMARY KEY (`ExternalColumnID`)
)
  ENGINE = InnoDB;

CREATE TABLE `RefreshProgress` (
  `ProgressID`         BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
  `BaseSourceVersion`  VARCHAR(255),
  `CreateTime`         DATETIME     NOT NULL,
  `CreatedBy`          VARCHAR(255) NOT NULL,
  `ErrorMessage`       VARCHAR(255),
  `LatestStatusUpdate` DATETIME     NOT NULL,
  `NumRetries`         INTEGER,
  `PivotDate`          DATETIME     NOT NULL,
  `RootOperationUID`   VARCHAR(255) NOT NULL UNIQUE,
  `RowsGenerated`      BIGINT       NOT NULL,
  `SourceName`         VARCHAR(255) NOT NULL,
  `Status`             VARCHAR(255) NOT NULL,
  `StatusBeforeFailed` VARCHAR(255),
  PRIMARY KEY (`ProgressID`),
  UNIQUE (`RootOperationUID`)
)
  ENGINE = InnoDB;

CREATE TABLE `SourceColumn` (
  `SourceColumnID` BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
  `Arguments`      VARCHAR(1000),
  `BaseSource`     VARCHAR(100),
  `Calculation`    VARCHAR(50)  NOT NULL,
  `ColumnName`     VARCHAR(100) NOT NULL,
  `ColumnType`     VARCHAR(50)  NOT NULL,
  `GroupBy`        VARCHAR(100),
  `Groups`         VARCHAR(255) NOT NULL,
  `Preparation`    VARCHAR(1000),
  `Priority`       INTEGER      NOT NULL,
  `SourceName`     VARCHAR(100) NOT NULL,
  PRIMARY KEY (`SourceColumnID`)
)
  ENGINE = InnoDB;

ALTER TABLE `ColumnMapping`
ADD INDEX FK9166C0784570DE77 (ExternalColumnID),
ADD CONSTRAINT FK9166C0784570DE77
FOREIGN KEY (ExternalColumnID)
REFERENCES `ExternalColumn` (`ExternalColumnID`)
  ON DELETE CASCADE;

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
