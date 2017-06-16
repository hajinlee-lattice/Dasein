USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdatePLay`()
  BEGIN
    CREATE TABLE `PLAY_LAUNCH` (
      `PID`                    BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `CREATED_TIMESTAMP`      DATETIME     NOT NULL,
      `DESCRIPTION`            VARCHAR(255),
      `LAST_UPDATED_TIMESTAMP` DATETIME     NOT NULL,
      `LAUNCH_ID`              VARCHAR(255) NOT NULL UNIQUE,
      `STATE`                  INTEGER      NOT NULL,
      `TABLE_NAME`             VARCHAR(255),
      `TENANT_ID`              BIGINT       NOT NULL,
      FK_PLAY_ID               BIGINT       NOT NULL,
      FK_TENANT_ID             BIGINT       NOT NULL,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `PLAY` (
      `PID`                    BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `DESCRIPTION`            VARCHAR(255),
      `DISPLAY_NAME`           VARCHAR(255) NOT NULL,
      `LAST_UPDATED_TIMESTAMP` DATETIME     NOT NULL,
      `NAME`                   VARCHAR(255) NOT NULL,
      `SEGMENT_NAME`           VARCHAR(255),
      `TENANT_ID`              BIGINT       NOT NULL,
      `TIMESTAMP`              DATETIME     NOT NULL,
      FK_CALL_PREP_ID          BIGINT,
      FK_TENANT_ID             BIGINT       NOT NULL,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `CALL_PREP` (
      `PID` BIGINT NOT NULL AUTO_INCREMENT UNIQUE,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;

    CREATE INDEX PLAY_LAUNCH_CREATED_TIME
      ON `PLAY_LAUNCH` (`CREATED_TIMESTAMP`);
    CREATE INDEX PLAY_LAUNCH_LAST_UPD_TIME
      ON `PLAY_LAUNCH` (`LAST_UPDATED_TIMESTAMP`);
    CREATE INDEX PLAY_LAUNCH_ID
      ON `PLAY_LAUNCH` (`LAUNCH_ID`);
    CREATE INDEX PLAY_LAUNCH_STATE
      ON `PLAY_LAUNCH` (`STATE`);
    ALTER TABLE `PLAY_LAUNCH`
      ADD INDEX FKF6CA4F1EE57F1489 (FK_PLAY_ID),
      ADD CONSTRAINT FKF6CA4F1EE57F1489 FOREIGN KEY (FK_PLAY_ID) REFERENCES `PLAY` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `PLAY_LAUNCH`
      ADD INDEX FKF6CA4F1E36865BC (FK_TENANT_ID),
      ADD CONSTRAINT FKF6CA4F1E36865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;

    ALTER TABLE `PLAY`
      ADD INDEX FK258334883752BA (FK_CALL_PREP_ID),
      ADD CONSTRAINT FK258334883752BA FOREIGN KEY (FK_CALL_PREP_ID) REFERENCES `CALL_PREP` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `PLAY`
      ADD INDEX FK25833436865BC (FK_TENANT_ID),
      ADD CONSTRAINT FK25833436865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateMetadata`()
  BEGIN
    ALTER TABLE `METADATA_DATA_COLLECTION_PROPERTY`
      DROP FOREIGN KEY FKA6E03D51D089E196;
    ALTER TABLE `METADATA_DATA_COLLECTION`
      DROP FOREIGN KEY FKF69DFD4336865BC;
    ALTER TABLE `METADATA_DATA_COLLECTION`
      DROP FOREIGN KEY FKF69DFD436816190C;
    ALTER TABLE `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY`
      DROP FOREIGN KEY FKD5E5A1AAFCF94C70;
    ALTER TABLE `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY`
      DROP FOREIGN KEY FKD5E5A1AAA326A20F;
    ALTER TABLE `METADATA_SEGMENT_PROPERTY`
      DROP FOREIGN KEY FKF9BE749179DE23EE;
    ALTER TABLE `METADATA_SEGMENT`
      DROP FOREIGN KEY FK90C89E03D089E196;

    DROP TABLE IF EXISTS `METADATA_DATA_COLLECTION_PROPERTY`;
    DROP TABLE IF EXISTS `METADATA_DATA_COLLECTION`;
    DROP TABLE IF EXISTS `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY`;
    DROP TABLE IF EXISTS `METADATA_SEGMENT_PROPERTY`;
    DROP TABLE IF EXISTS `METADATA_SEGMENT`;

    CREATE TABLE `METADATA_DATA_COLLECTION_PROPERTY` (
      `PID`                 BIGINT       NOT NULL AUTO_INCREMENT,
      `PROPERTY`            VARCHAR(255) NOT NULL,
      `VALUE`               VARCHAR(2048),
      FK_DATA_COLLECTION_ID BIGINT       NOT NULL,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `METADATA_DATA_COLLECTION_TABLE` (
      `PID`            BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `ROLE`           VARCHAR(255) NOT NULL,
      `TENANT_ID`      BIGINT       NOT NULL,
      FK_COLLECTION_ID BIGINT       NOT NULL,
      FK_TABLE_ID      BIGINT       NOT NULL,
      FK_TENANT_ID     BIGINT       NOT NULL,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `METADATA_DATA_COLLECTION` (
      `PID`        BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `NAME`       VARCHAR(255) NOT NULL,
      `TENANT_ID`  BIGINT       NOT NULL,
      `TYPE`       VARCHAR(255) NOT NULL,
      FK_TENANT_ID BIGINT       NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE (`TENANT_ID`, `NAME`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY` (
      FK_ATTRIBUTE_ID BIGINT NOT NULL,
      FK_SEGMENT_ID   BIGINT NOT NULL
    )
      ENGINE = InnoDB;
    CREATE TABLE `METADATA_SEGMENT_PROPERTY` (
      `PID`               BIGINT       NOT NULL AUTO_INCREMENT,
      `PROPERTY`          VARCHAR(255) NOT NULL,
      `VALUE`             VARCHAR(2048),
      METADATA_SEGMENT_ID BIGINT       NOT NULL,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `METADATA_SEGMENT` (
      `PID`               BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `CREATED`           DATETIME     NOT NULL,
      `DESCRIPTION`       VARCHAR(255),
      `DISPLAY_NAME`      VARCHAR(255) NOT NULL,
      `IS_MASTER_SEGMENT` BOOLEAN      NOT NULL,
      `NAME`              VARCHAR(255) NOT NULL,
      `RESTRICTION`       LONGTEXT,
      `TENANT_ID`         BIGINT       NOT NULL,
      `UPDATED`           DATETIME     NOT NULL,
      FK_COLLECTION_ID    BIGINT       NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE (`TENANT_ID`, `NAME`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `METADATA_STATISTICS` (
      `PID`         BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `DATA`        LONGBLOB     NOT NULL,
      `MODEL`       VARCHAR(255),
      `NAME`        VARCHAR(255) NOT NULL UNIQUE,
      `TENANT_ID`   BIGINT       NOT NULL,
      FK_SEGMENT_ID BIGINT       NOT NULL,
      FK_TENANT_ID  BIGINT       NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE (`TENANT_ID`, `NAME`)
    )
      ENGINE = InnoDB;
    CREATE INDEX IX_PROPERTY
      ON `METADATA_DATA_COLLECTION_PROPERTY` (`PROPERTY`);
    ALTER TABLE `METADATA_DATA_COLLECTION_PROPERTY`
      ADD INDEX FKA6E03D51D089E196 (FK_DATA_COLLECTION_ID),
      ADD CONSTRAINT FKA6E03D51D089E196 FOREIGN KEY (FK_DATA_COLLECTION_ID) REFERENCES `METADATA_DATA_COLLECTION` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_DATA_COLLECTION_TABLE`
      ADD INDEX FKD48865B27BF7C1B7 (FK_COLLECTION_ID),
      ADD CONSTRAINT FKD48865B27BF7C1B7 FOREIGN KEY (FK_COLLECTION_ID) REFERENCES `METADATA_DATA_COLLECTION` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_DATA_COLLECTION_TABLE`
      ADD INDEX FKD48865B25FC50F27 (FK_TABLE_ID),
      ADD CONSTRAINT FKD48865B25FC50F27 FOREIGN KEY (FK_TABLE_ID) REFERENCES `METADATA_TABLE` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_DATA_COLLECTION_TABLE`
      ADD INDEX FKD48865B236865BC (FK_TENANT_ID),
      ADD CONSTRAINT FKD48865B236865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_DATA_COLLECTION`
      ADD INDEX FKF69DFD4336865BC (FK_TENANT_ID),
      ADD CONSTRAINT FKF69DFD4336865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY`
      ADD INDEX FKD5E5A1AAFCF94C70 (FK_SEGMENT_ID),
      ADD CONSTRAINT FKD5E5A1AAFCF94C70 FOREIGN KEY (FK_SEGMENT_ID) REFERENCES `METADATA_ATTRIBUTE` (`PID`);
    ALTER TABLE `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY`
      ADD INDEX FKD5E5A1AAA326A20F (FK_ATTRIBUTE_ID),
      ADD CONSTRAINT FKD5E5A1AAA326A20F FOREIGN KEY (FK_ATTRIBUTE_ID) REFERENCES `METADATA_SEGMENT` (`PID`);
    CREATE INDEX IX_PROPERTY
      ON `METADATA_SEGMENT_PROPERTY` (`PROPERTY`);
    ALTER TABLE `METADATA_SEGMENT_PROPERTY`
      ADD INDEX FKF9BE749179DE23EE (METADATA_SEGMENT_ID),
      ADD CONSTRAINT FKF9BE749179DE23EE FOREIGN KEY (METADATA_SEGMENT_ID) REFERENCES `METADATA_SEGMENT` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_SEGMENT`
      ADD INDEX FK90C89E037BF7C1B7 (FK_COLLECTION_ID),
      ADD CONSTRAINT FK90C89E037BF7C1B7 FOREIGN KEY (FK_COLLECTION_ID) REFERENCES `METADATA_DATA_COLLECTION` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_STATISTICS`
      ADD INDEX FK5C2E0433E88DD898 (FK_SEGMENT_ID),
      ADD CONSTRAINT FK5C2E0433E88DD898 FOREIGN KEY (FK_SEGMENT_ID) REFERENCES `METADATA_SEGMENT` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `METADATA_STATISTICS`
      ADD INDEX FK5C2E043336865BC (FK_TENANT_ID),
      ADD CONSTRAINT FK5C2E043336865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateDataFeedTables`()
  BEGIN
    CREATE TABLE `DATAFEED_EXECUTION` (
      `PID`         BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `STATUS`      VARCHAR(255) NOT NULL,
      `WORKFLOW_ID` BIGINT,
      FK_FEED_ID    BIGINT       NOT NULL,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `DATAFEED_IMPORT` (
      `PID`             BIGINT        NOT NULL AUTO_INCREMENT UNIQUE,
      `ENTITY`          VARCHAR(255)  NOT NULL,
      `FEED_TYPE`       VARCHAR(255),
      `SOURCE`          VARCHAR(255)  NOT NULL,
      `SOURCE_CONFIG`   VARCHAR(1000) NOT NULL,
      `START_TIME`      DATETIME      NOT NULL,
      `FK_FEED_EXEC_ID` BIGINT        NOT NULL,
      FK_DATA_ID        BIGINT        NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE (`SOURCE`, `ENTITY`, `FEED_TYPE`, `FK_FEED_EXEC_ID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `DATAFEED_TASK_TABLE` (
      `PID`         BIGINT NOT NULL AUTO_INCREMENT UNIQUE,
      `FK_TASK_ID`  BIGINT NOT NULL,
      `FK_TABLE_ID` BIGINT NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE (`FK_TASK_ID`, `FK_TABLE_ID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `DATAFEED_TASK` (
      `PID`           BIGINT        NOT NULL AUTO_INCREMENT UNIQUE,
      `ACTIVE_JOB`    VARCHAR(255)  NOT NULL,
      `ENTITY`        VARCHAR(255)  NOT NULL,
      `FEED_TYPE`     VARCHAR(255),
      `LAST_IMPORTED` DATETIME      NOT NULL,
      `SOURCE`        VARCHAR(255)  NOT NULL,
      `SOURCE_CONFIG` VARCHAR(1000) NOT NULL,
      `START_TIME`    DATETIME      NOT NULL,
      `STATUS`        VARCHAR(255)  NOT NULL,
      `FK_FEED_ID`    BIGINT        NOT NULL,
      FK_DATA_ID      BIGINT,
      FK_TEMPLATE_ID  BIGINT        NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE (`SOURCE`, `ENTITY`, `FEED_TYPE`, `FK_FEED_ID`)
    )
      ENGINE = InnoDB;
    CREATE TABLE `DATAFEED` (
      `PID`              BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `ACTIVE_EXECUTION` BIGINT       NOT NULL,
      `NAME`             VARCHAR(255) NOT NULL,
      `STATUS`           VARCHAR(255) NOT NULL,
      `TENANT_ID`        BIGINT       NOT NULL,
      FK_COLLECTION_ID   BIGINT       NOT NULL,
      FK_TENANT_ID       BIGINT       NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE (`TENANT_ID`, `NAME`)
    )
      ENGINE = InnoDB;

    ALTER TABLE `DATAFEED_EXECUTION`
      ADD INDEX FK3E5D3BC1679FF377 (FK_FEED_ID),
      ADD CONSTRAINT FK3E5D3BC1679FF377 FOREIGN KEY (FK_FEED_ID) REFERENCES `DATAFEED` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `DATAFEED_IMPORT`
      ADD INDEX FKADAC237C56FF4425 (`FK_FEED_EXEC_ID`),
      ADD CONSTRAINT FKADAC237C56FF4425 FOREIGN KEY (`FK_FEED_EXEC_ID`) REFERENCES `DATAFEED_EXECUTION` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `DATAFEED_IMPORT`
      ADD INDEX FKADAC237C525FEA17 (FK_DATA_ID),
      ADD CONSTRAINT FKADAC237C525FEA17 FOREIGN KEY (FK_DATA_ID) REFERENCES `METADATA_TABLE` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `DATAFEED_TASK_TABLE`
      ADD INDEX FKF200D4CBD3C51D55 (`FK_TASK_ID`),
      ADD CONSTRAINT FKF200D4CBD3C51D55 FOREIGN KEY (`FK_TASK_ID`) REFERENCES `DATAFEED_TASK` (`PID`);
    ALTER TABLE `DATAFEED_TASK_TABLE`
      ADD INDEX FKF200D4CB68A6FB47 (`FK_TABLE_ID`),
      ADD CONSTRAINT FKF200D4CB68A6FB47 FOREIGN KEY (`FK_TABLE_ID`) REFERENCES `METADATA_TABLE` (`PID`);
    ALTER TABLE `DATAFEED_TASK`
      ADD INDEX FK7679F31C80AFF377 (`FK_FEED_ID`),
      ADD CONSTRAINT FK7679F31C80AFF377 FOREIGN KEY (`FK_FEED_ID`) REFERENCES `DATAFEED` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `DATAFEED_TASK`
      ADD INDEX FK7679F31C525FEA17 (FK_DATA_ID),
      ADD CONSTRAINT FK7679F31C525FEA17 FOREIGN KEY (FK_DATA_ID) REFERENCES `METADATA_TABLE` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `DATAFEED_TASK`
      ADD INDEX FK7679F31C15766847 (FK_TEMPLATE_ID),
      ADD CONSTRAINT FK7679F31C15766847 FOREIGN KEY (FK_TEMPLATE_ID) REFERENCES `METADATA_TABLE` (`PID`)
      ON DELETE CASCADE;
    CREATE INDEX IX_FEED_NAME
      ON `DATAFEED` (`NAME`);
    ALTER TABLE `DATAFEED`
      ADD INDEX FK9950E0487BF7C1B7 (FK_COLLECTION_ID),
      ADD CONSTRAINT FK9950E0487BF7C1B7 FOREIGN KEY (FK_COLLECTION_ID) REFERENCES `METADATA_DATA_COLLECTION` (`PID`)
      ON DELETE CASCADE;
    ALTER TABLE `DATAFEED`
      ADD INDEX FK9950E04836865BC (FK_TENANT_ID),
      ADD CONSTRAINT FK9950E04836865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateModelNotes`()
  BEGIN
    CREATE TABLE `MODEL_NOTES` (
      `PID`                         BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
      `CREATED_BY_USER`             VARCHAR(255),
      `CREATION_TIMESTAMP`          BIGINT,
      `ID`                          VARCHAR(255) NOT NULL UNIQUE,
      `LAST_MODIFICATION_TIMESTAMP` BIGINT,
      `LAST_MODIFIED_BY_USER`       VARCHAR(255),
      `NOTES_CONTENTS`              VARCHAR(2048),
      `ORIGIN`                      VARCHAR(255),
      `PARENT_MODEL_ID`             VARCHAR(255),
      MODEL_ID                      BIGINT       NOT NULL,
      PRIMARY KEY (`PID`)
    )
      ENGINE = InnoDB;
    CREATE INDEX MODEL_NOTES_ID_IDX
      ON `MODEL_NOTES` (`ID`);
    ALTER TABLE `MODEL_NOTES`
      ADD INDEX FKE83891EB815935F7 (MODEL_ID),
      ADD CONSTRAINT FKE83891EB815935F7 FOREIGN KEY (MODEL_ID) REFERENCES `MODEL_SUMMARY` (`PID`)
      ON DELETE CASCADE;
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    START TRANSACTION;
    CALL `UpdatePLay`();
    CALL `UpdateMetadata`();
    CALL `UpdateDataFeedTables`();
    CALL `UpdateModelNotes`();
    COMMIT;
  END;
//
DELIMITER ;

CALL `UpdateSchema`();



