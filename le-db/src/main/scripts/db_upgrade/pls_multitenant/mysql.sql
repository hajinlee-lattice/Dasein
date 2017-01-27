USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DROP PROCEDURE IF EXISTS `UpdateMetadataTable`;

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

    # create table METADATA_TABLE_TAG if not exists
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

DROP PROCEDURE IF EXISTS `UpdateModelQualityAlgorithm`;

DELIMITER //
CREATE PROCEDURE `UpdateModelQualityAlgorithm`()
  BEGIN

    # add column if not exists
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

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN

    CALL `UpdateMetadataTable`();
    CALL `CreateMetadataTableTag`();
    CALL `CreateMetadataStorage`();
    CALL `UpdateModelQualityAlgorithm`();

  END //
DELIMITER ;

CALL `UpdateSchema`();



