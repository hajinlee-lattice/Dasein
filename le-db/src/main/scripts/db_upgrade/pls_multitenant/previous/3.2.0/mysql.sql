USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DROP PROCEDURE IF EXISTS `UpdateMetadataTable`;

DROP PROCEDURE IF EXISTS `UpdateModelSummaryDownloadFlag`;

DELIMITER //
CREATE PROCEDURE `UpdateMetadataTable`()
  BEGIN

    # add column if not exists
    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE
                    TABLE_SCHEMA = 'PLS_MultiTenant'
                    AND TABLE_NAME = 'METADATA_TABLE'
                    AND COLUMN_NAME = 'NAMESPACE')
    THEN
      ALTER TABLE `METADATA_TABLE`
        ADD `NAMESPACE` VARCHAR(255);
    END IF;

  END //
DELIMITER ;

DROP PROCEDURE IF EXISTS `CreateMetadataTableTag`;

DELIMITER //
CREATE PROCEDURE `CreateMetadataTableTag`()
  BEGIN
    DECLARE dbName VARCHAR(20) DEFAULT 'PLS_MultiTenant';

    # create table METADATA_TABLE_TAG if not exists
    IF NOT EXISTS(SELECT *
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_SCHEMA = dbName
                        AND TABLE_NAME = 'METADATA_TABLE_TAG')
    THEN
      CREATE TABLE `METADATA_TABLE_TAG` (
        `PID`       BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
        `NAME`      VARCHAR(255) NOT NULL,
        `TENANT_ID` BIGINT       NOT NULL,
        FK_TABLE_ID BIGINT       NOT NULL,
        PRIMARY KEY (`PID`)
      )
        ENGINE = InnoDB;
    END IF;

    # recreate foreign key constrain FK6BACA6595FC50F27
    IF EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
              WHERE table_schema = dbName
                    AND TABLE_NAME = 'METADATA_TABLE_TAG'
                    AND CONSTRAINT_NAME = 'FK6BACA6595FC50F27')
    THEN
      ALTER TABLE `METADATA_TABLE_TAG`
        DROP FOREIGN KEY `FK6BACA6595FC50F27`;
    END IF;
    IF EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.STATISTICS
              WHERE TABLE_SCHEMA = dbName
                    AND TABLE_NAME = 'METADATA_TABLE_TAG'
                    AND INDEX_NAME = 'FK6BACA6595FC50F27')
    THEN
      ALTER TABLE `METADATA_TABLE_TAG`
        DROP INDEX `FK6BACA6595FC50F27`;
    END IF;
    ALTER TABLE `METADATA_TABLE_TAG`
      ADD INDEX FK6BACA6595FC50F27 (FK_TABLE_ID),
      ADD CONSTRAINT FK6BACA6595FC50F27 FOREIGN KEY (FK_TABLE_ID) REFERENCES `METADATA_TABLE` (`PID`)
      ON DELETE CASCADE;

  END //
DELIMITER ;

DROP PROCEDURE IF EXISTS `CreateMetadataStorage`;

DELIMITER //
CREATE PROCEDURE `CreateMetadataStorage`()
  BEGIN
    DECLARE dbName VARCHAR(20) DEFAULT 'PLS_MultiTenant';

    # create table METADATA_STORAGE if not exists
    IF NOT EXISTS(SELECT table_name
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_SCHEMA = dbName
                        AND TABLE_NAME = 'METADATA_STORAGE')
    THEN
      CREATE TABLE `METADATA_STORAGE` (
        `NAME`          VARCHAR(31)  NOT NULL,
        `PID`           BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
        `TABLE_NAME`    VARCHAR(255) NOT NULL,
        `DATABASE_NAME` INTEGER,
        FK_TABLE_ID     BIGINT       NOT NULL,
        PRIMARY KEY (`PID`)
      )
        ENGINE = InnoDB;

      INSERT INTO `METADATA_STORAGE` (NAME, TABLE_NAME, FK_TABLE_ID)
        SELECT
          'HDFS',
          `NAME`,
          `PID`
        FROM `METADATA_TABLE`;
    END IF;

    # recreate index FKAAD4414B5FC50F27
    IF EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
              WHERE table_schema = dbName
                    AND TABLE_NAME = 'METADATA_STORAGE'
                    AND CONSTRAINT_NAME = 'FKAAD4414B5FC50F27')
    THEN
      ALTER TABLE `METADATA_STORAGE`
        DROP FOREIGN KEY `FKAAD4414B5FC50F27`;
    END IF;

    IF EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.STATISTICS
              WHERE table_schema = dbName
                    AND TABLE_NAME = 'METADATA_STORAGE'
                    AND INDEX_NAME = 'FKAAD4414B5FC50F27')
    THEN
      ALTER TABLE `METADATA_STORAGE`
        DROP INDEX `FKAAD4414B5FC50F27`;
    END IF;

    ALTER TABLE `METADATA_STORAGE`
      ADD INDEX FKAAD4414B5FC50F27 (FK_TABLE_ID),
      ADD CONSTRAINT FKAAD4414B5FC50F27 FOREIGN KEY (FK_TABLE_ID) REFERENCES `METADATA_TABLE` (`PID`)
      ON DELETE CASCADE;

  END //
DELIMITER ;

DROP PROCEDURE IF EXISTS `CreateBucketMetadaTable`;

DELIMITER //
CREATE PROCEDURE `CreateBucketMetadaTable`()
  BEGIN
    DECLARE dbName VARCHAR(20) DEFAULT 'PLS_MultiTenant';

    # create table BUCKET_METADATA if not exists
    IF NOT EXISTS(SELECT table_name
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE TABLE_SCHEMA = dbName
                        AND TABLE_NAME = 'BUCKET_METADATA')
    THEN
      CREATE TABLE `BUCKET_METADATA` (
        `NAME`          VARCHAR(31)  NOT NULL,
        `PID`           BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
        `CREATION_TIMESTAMP` BIGINT NOT NULL,
        `LEFT_BOUND_SCORE` INTEGER NOT NULL,
        `LIFT` DOUBLE PRECISION NOT NULL,
        `MODEL_ID` VARCHAR(255) NOT NULL,
        `NUM_LEADS` INTEGER NOT NULL,
        `RIGHT_BOUND_SCORE` INTEGER NOT NULL,
        `TENANT_ID` BIGINT NOT NULL,
        FK_TENANT_ID BIGINT NOT NULL,
        PRIMARY KEY (`PID`)
      )
        ENGINE = InnoDB;

    END IF;

    # recreate index FK398165E436865BC
    IF EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
              WHERE table_schema = dbName
                    AND TABLE_NAME = 'BUCKET_METADATA'
                    AND CONSTRAINT_NAME = 'FK398165E436865BC')
    THEN
      ALTER TABLE `BUCKET_METADATA`
        DROP FOREIGN KEY `FK398165E436865BC`;
    END IF;

    IF EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.STATISTICS
              WHERE table_schema = dbName
                    AND TABLE_NAME = 'BUCKET_METADATA'
                    AND INDEX_NAME = 'FK398165E436865BC')
    THEN
      ALTER TABLE `BUCKET_METADATA`
        DROP INDEX `FK398165E436865BC`;
    END IF;

    ALTER TABLE `BUCKET_METADATA`
      ADD INDEX FK398165E436865BC (FK_TENANT_ID),
      ADD CONSTRAINT FK398165E436865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;

  END //
DELIMITER ;

DROP PROCEDURE IF EXISTS `UpdateModelQualityAlgorithm`;

DELIMITER //
CREATE PROCEDURE `UpdateModelQualityAlgorithm`()
  BEGIN

    # add column if it does not exist
    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE
                    TABLE_SCHEMA = 'PLS_MultiTenant'
                    AND TABLE_NAME = 'MODELQUALITY_ALGORITHM'
                    AND COLUMN_NAME = 'TYPE')
    THEN
      ALTER TABLE `MODELQUALITY_ALGORITHM`
        ADD `TYPE` INT(11) DEFAULT NULL;
    END IF;

  END //
DELIMITER ;

DROP PROCEDURE IF EXISTS `UpdateModelSummary`;

DELIMITER //
CREATE PROCEDURE `UpdateModelSummary`()
  BEGIN

    # add columns if they do not exist
    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE
                    TABLE_SCHEMA = 'PLS_MultiTenant'
                    AND TABLE_NAME = 'MODEL_SUMMARY'
                    AND COLUMN_NAME = 'CROSS_VALIDATION_MEAN')
    THEN
      ALTER TABLE `MODEL_SUMMARY`
        ADD `CROSS_VALIDATION_MEAN` DOUBLE DEFAULT NULL;
    END IF;

    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE
                    TABLE_SCHEMA = 'PLS_MultiTenant'
                    AND TABLE_NAME = 'MODEL_SUMMARY'
                    AND COLUMN_NAME = 'CROSS_VALIDATION_STD')
    THEN
      ALTER TABLE `MODEL_SUMMARY`
        ADD `CROSS_VALIDATION_STD` DOUBLE DEFAULT NULL;
    END IF;

  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateModelSummaryDownloadFlag`()
  BEGIN
    IF NOT EXISTS(SELECT * 
              FROM information_schema.STATISTICS
              WHERE TABLE_SCHEMA = 'PLS_MultiTenant'
              AND TABLE_NAME = 'MODEL_SUMMARY_DOWNLOAD_FLAGS'
              AND INDEX_NAME = 'IX_MARK_TIME')
    THEN
      ALTER TABLE `MODEL_SUMMARY_DOWNLOAD_FLAGS`
        ADD INDEX IX_MARK_TIME (MARK_TIME);
    END IF;
    IF NOT EXISTS(SELECT * 
              FROM information_schema.STATISTICS
              WHERE TABLE_SCHEMA = 'PLS_MultiTenant'
              AND TABLE_NAME = 'MODEL_SUMMARY_DOWNLOAD_FLAGS'
              AND INDEX_NAME = 'IX_TENANT_ID')
    THEN
      ALTER TABLE `MODEL_SUMMARY_DOWNLOAD_FLAGS`
        ADD INDEX IX_TENANT_ID (Tenant_ID);
    END IF;
  END //
DELIMITER ;

DROP PROCEDURE IF EXISTS `CreateCustomization`;

DELIMITER //
CREATE PROCEDURE `CreateCustomization`()
  BEGIN
    IF NOT EXISTS(SELECT * 
              FROM information_schema.STATISTICS
              WHERE TABLE_SCHEMA = 'PLS_MultiTenant'
              AND TABLE_NAME = 'ATTRIBUTE_CUSTOMIZATION_PROPERTY'
              AND INDEX_NAME = 'IX_TENANT_ATTRIBUTENAME_USECASE')
    THEN
      create table `ATTRIBUTE_CUSTOMIZATION_PROPERTY` (`PID` bigint not null auto_increment unique, `ATTRIBUTE_NAME` varchar(100) not null, `CATEGORY_NAME` varchar(255) not null, `PROPERTY_NAME` varchar(255) not null, `PROPERTY_VALUE` varchar(255), `TENANT_PID` bigint not null, `USE_CASE` varchar(255) not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_PID`, `ATTRIBUTE_NAME`, `USE_CASE`, `CATEGORY_NAME`, `PROPERTY_NAME`));
      create index IX_TENANT_ATTRIBUTENAME_USECASE on `ATTRIBUTE_CUSTOMIZATION_PROPERTY` (`ATTRIBUTE_NAME`, FK_TENANT_ID, `USE_CASE`);
      alter table `ATTRIBUTE_CUSTOMIZATION_PROPERTY` add index FKBFFD520436865BC (FK_TENANT_ID), add constraint FKBFFD520436865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`);
    END IF;
    IF NOT EXISTS(SELECT * 
              FROM information_schema.STATISTICS
              WHERE TABLE_SCHEMA = 'PLS_MultiTenant'
              AND TABLE_NAME = 'CATEGORY_CUSTOMIZATION_PROPERTY'
              AND INDEX_NAME = 'IX_TENANT_ATTRIBUTECATEGORY_USECASE')
    THEN
      create table `CATEGORY_CUSTOMIZATION_PROPERTY` (`PID` bigint not null auto_increment unique, `CATEGORY_NAME` varchar(255) not null, `PROPERTY_NAME` varchar(255) not null, `PROPERTY_VALUE` varchar(255), `TENANT_PID` bigint not null, `USE_CASE` varchar(255) not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_PID`, `USE_CASE`, `CATEGORY_NAME`, `PROPERTY_NAME`));
      create index IX_TENANT_ATTRIBUTECATEGORY_USECASE on `CATEGORY_CUSTOMIZATION_PROPERTY` (FK_TENANT_ID, `USE_CASE`);
      alter table `CATEGORY_CUSTOMIZATION_PROPERTY` add index FK60EAD4236865BC (FK_TENANT_ID), add constraint FK60EAD4236865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`);
    END IF;
  END //
DELIMITER ;


DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN

    CALL `UpdateMetadataTable`();
    CALL `CreateMetadataTableTag`();
    CALL `CreateMetadataStorage`();
    CALL `CreateBucketMetadaTable`();
    CALL `UpdateModelQualityAlgorithm`();
    CALL `UpdateModelSummary`();
    CALL `UpdateModelSummaryDownloadFlag`();
    CALL `CreateCustomization`();

  END //
DELIMITER ;

CALL `UpdateSchema`();



