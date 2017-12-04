CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN
        CREATE TABLE `CDL_JOB_DETAIL` (
            `PID`              BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
            `APPLICATION_ID`   VARCHAR(255),
            `CDL_JOB_STATUS`   VARCHAR(255) NOT NULL,
            `CDL_JOB_TYPE`     VARCHAR(255) NOT NULL,
            `CREATE_DATE`      DATETIME,
            `LAST_UPDATE_DATE` DATETIME,
            `RETRY_COUNT`      INTEGER      NOT NULL,
            `TENANT_ID`        BIGINT       NOT NULL,
            FK_TENANT_ID       BIGINT       NOT NULL,
            PRIMARY KEY (`PID`)
        )
            ENGINE = InnoDB;

        ALTER TABLE `CDL_JOB_DETAIL`
            ADD INDEX FK41487BE736865BC (FK_TENANT_ID),
            ADD CONSTRAINT FK41487BE736865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`)
            ON DELETE CASCADE;

        ALTER TABLE `PLS_MultiTenant`.`DATAFEED`
            ADD COLUMN `AUTO_SCHEDULING` boolean not null DEFAULT 0,
            ADD COLUMN `DRAINING_STATUS` varchar(255) not null DEFAULT 'NONE';

        ALTER TABLE `PLS_MultiTenant`.`SOURCE_FILE`
            CHANGE COLUMN `ENTITY_EXTERNAL_TYPE` `BUSINESS_ENTITY` VARCHAR(255);
            
        ALTER TABLE `PLS_MultiTenant`.`AI_MODEL`
        		ADD COLUMN `MODELING_METHOD` varchar(255),
            ADD COLUMN `MODELING_CONFIG_FILTERS` longtext,
            ADD COLUMN `MODELING_JOBID` varchar(255),
            ADD COLUMN `TARGET_CUSTOMER_SET` varchar(255),
		    ADD COLUMN `TARGET_PRODUCTS` longtext,
		    ADD COLUMN `TRAINING_PRODUCTS` longtext,
		    ADD COLUMN `WORKFLOW_TYPE` varchar(255),
		    ADD COLUMN `FK_TRAINING_SEGMENT_ID` bigint,
		    ADD COLUMN `FK_MODEL_SUMMARY_ID` bigint;
		    
		ALTER TABLE `PLS_MultiTenant`.`AI_MODEL` ADD CONSTRAINT `FKk2ahhvijs5146b4g5e9jrgeuq` foreign key (`FK_TRAINING_SEGMENT_ID`) references `PLS_MultiTenant`.`METADATA_SEGMENT` (`PID`) on delete cascade;
		ALTER TABLE `PLS_MultiTenant`.`AI_MODEL` ADD CONSTRAINT `FKgenp90xodrrj475g7g7xcxoti` foreign key (`FK_MODEL_SUMMARY_ID`) references `PLS_MultiTenant`.`MODEL_SUMMARY` (`PID`);
    
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdatePlayTables`()
    BEGIN
        ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
            ADD COLUMN `TABLE_NAME` varchar(255);
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateSegmentExportTables`()
    BEGIN
        CREATE TABLE `METADATA_SEGMENT_EXPORT` (
          `PID` bigint(20) NOT NULL AUTO_INCREMENT,
          `APPLICATION_ID` varchar(255) DEFAULT NULL,
          `CLEANUP_BY` datetime NOT NULL,
          `CONTACT_RESTRICTION` longtext,
          `CREATED` datetime NOT NULL,
          `CREATED_BY` varchar(255) DEFAULT NULL,
          `EXPORT_ID` varchar(255) NOT NULL,
          `TYPE` varchar(255) NOT NULL,
          `FILE_NAME` varchar(255) NOT NULL,
          `PATH` varchar(2048) NOT NULL,
          `RESTRICTION` longtext,
          `STATUS` varchar(255) NOT NULL,
          `TABLE_NAME` varchar(255) NOT NULL,
          `TENANT_ID` bigint(20) NOT NULL,
          `UPDATED` datetime NOT NULL,
          `FK_SEGMENT_ID` bigint(20) DEFAULT NULL,
          `FK_TENANT_ID` bigint(20) NOT NULL,
          PRIMARY KEY (`PID`),
          UNIQUE KEY `EXPORT_ID` (`EXPORT_ID`)
        )
            ENGINE = InnoDB;

        ALTER TABLE `METADATA_SEGMENT_EXPORT`
            ADD INDEX `FKCBB92D70E88DD898` (`FK_SEGMENT_ID`),
            ADD CONSTRAINT `FKCBB92D70E88DD898` FOREIGN KEY (`FK_SEGMENT_ID`) REFERENCES `METADATA_SEGMENT` (`PID`) 
            ON DELETE CASCADE;

        ALTER TABLE `METADATA_SEGMENT_EXPORT`
            ADD INDEX `FKCBB92D7036865BC` (`FK_TENANT_ID`),
            ADD CONSTRAINT `FKCBB92D7036865BC` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`)
            ON DELETE CASCADE;

        ALTER TABLE `METADATA_SEGMENT_EXPORT`
          ADD INDEX `METADATA_SEGMENT_EXPORT_TTL` (`CLEANUP_BY`);

        ALTER TABLE `METADATA_SEGMENT_EXPORT`
          ADD INDEX `METADATA_SEGMENT_EXPORT_ID` (`EXPORT_ID`);
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateNoteTables`()
    BEGIN
        CREATE TABLE `RATING_ENGINE_NOTE` (
            `PID`                         BIGINT       NOT NULL AUTO_INCREMENT,
            `CREATED_BY_USER`             VARCHAR(255),
            `CREATION_TIMESTAMP`          BIGINT       NOT NULL,
            `ID`                          VARCHAR(255) NOT NULL UNIQUE,
            `LAST_MODIFICATION_TIMESTAMP` BIGINT       NOT NULL,
            `LAST_MODIFIED_BY_USER`       VARCHAR(255),
            `NOTES_CONTENTS`              VARCHAR(2048),
            `ORIGIN`                      VARCHAR(255),
            FK_RATING_ENGINE_ID           BIGINT       NOT NULL,
            PRIMARY KEY (`PID`)
        )
            ENGINE = InnoDB;
        ALTER TABLE `RATING_ENGINE_NOTE`
            ADD INDEX FKCF998C2D94623258 (FK_RATING_ENGINE_ID),
            ADD CONSTRAINT FKCF998C2D94623258 FOREIGN KEY (FK_RATING_ENGINE_ID) REFERENCES `RATING_ENGINE` (`PID`)
            ON DELETE CASCADE;

        CREATE TABLE `MODEL_NOTE` (
            `PID`                         BIGINT       NOT NULL AUTO_INCREMENT,
            `CREATED_BY_USER`             VARCHAR(255),
            `CREATION_TIMESTAMP`          BIGINT       NOT NULL,
            `ID`                          VARCHAR(255) NOT NULL UNIQUE,
            `LAST_MODIFICATION_TIMESTAMP` BIGINT       NOT NULL,
            `LAST_MODIFIED_BY_USER`       VARCHAR(255),
            `NOTES_CONTENTS`              VARCHAR(2048),
            `ORIGIN`                      VARCHAR(255),
            `PARENT_MODEL_ID`             VARCHAR(255),
            MODEL_ID                      BIGINT       NOT NULL,
            PRIMARY KEY (`PID`)
        )
            ENGINE = InnoDB;
        ALTER TABLE `MODEL_NOTE`
            ADD INDEX FK9C22DB68815935F7 (MODEL_ID),
            ADD CONSTRAINT FK9C22DB68815935F7 FOREIGN KEY (MODEL_ID) REFERENCES `MODEL_SUMMARY` (`PID`)
            ON DELETE CASCADE;

        INSERT INTO `MODEL_NOTE` (`CREATED_BY_USER`, `CREATION_TIMESTAMP`, `ID`, `LAST_MODIFICATION_TIMESTAMP`, `LAST_MODIFIED_BY_USER`, `NOTES_CONTENTS`, `ORIGIN`, `MODEL_ID`, `PARENT_MODEL_ID`)
            SELECT
                `CREATED_BY_USER`,
                `CREATION_TIMESTAMP`,
                `ID`,
                `LAST_MODIFICATION_TIMESTAMP`,
                `LAST_MODIFIED_BY_USER`,
                `NOTES_CONTENTS`,
                `ORIGIN`,
                `MODEL_ID`,
                `PARENT_MODEL_ID`
            FROM `MODEL_NOTES`;
    END;
//
DELIMITER ;


CREATE PROCEDURE `UpdateMetadataAttribute`()
    BEGIN
        ALTER TABLE `METADATA_ATTRIBUTE` ADD COLUMN `GROUPS` VARCHAR(1000);
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateSegmentAndRatingEngine`()
    BEGIN
        ALTER TABLE `METADATA_SEGMENT`
            ADD COLUMN `ACCOUNTS` BIGINT,
            ADD COLUMN `CONTACTS` BIGINT,
            ADD COLUMN `PRODUCTS` BIGINT;

        ALTER TABLE `RATING_ENGINE`
            ADD COLUMN `COUNTS`  VARCHAR(1000);
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateWorkflowJob`()
    BEGIN
        ALTER TABLE `WORKFLOW_JOB`
            ADD COLUMN `PARENT_JOB_ID` BIGINT,
            ADD COLUMN `TYPE` varchar(255);
    END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
    BEGIN
        START TRANSACTION;
        CALL `UpdateCDLTables`();
        CALL `UpdatePlayTables`();
        CALL `UpdateSegmentExportTables`();
        CALL `UpdateNoteTables`();
        CALL `UpdateMetadataAttribute`();
        CALL `UpdateSegmentAndRatingEngine`();
        CALL `UpdatePlayTables`();
        CALL `UpdateWorkflowJob`();
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
