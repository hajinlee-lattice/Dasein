/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script. This file DDL/DML can be applied at 'release regression' & 'release window' cycle
* Rule - Contains DDL/DML sql queries.  Should maintain backward compatibility.
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      ALTER TABLE `DCP_UPLOAD` ADD COLUMN `UPLOAD_DIAGNOSTICS` JSON;

      CREATE TABLE `DCP_DATA_REPORT`
        (
           `PID`                     BIGINT NOT NULL auto_increment,
           `BASIC_STATS`             JSON,
           `CREATED`                 DATETIME NOT NULL,
           `DATA_SNAPSHOT_TIME`      DATETIME,
           `DUPLICATION_REPORT`      JSON,
           `GEO_DISTRIBUTION_REPORT` JSON,
           `INPUT_PRESENCE_REPORT`   JSON,
           `LEVEL`                   VARCHAR(20),
           `MATCH_TO_DUNS_REPORT`    JSON,
           `OWNER_ID`                VARCHAR(255) NOT NULL,
           `PARENT_ID`               BIGINT,
           `REFRESH_TIME`            DATETIME,
           `UPDATED`                 DATETIME NOT NULL,
           `FK_TENANT_ID`            BIGINT NOT NULL,
           PRIMARY KEY (`PID`)
        )
      engine=InnoDB;

      CREATE INDEX IX_OWNER_ID_LEVEL ON `DCP_DATA_REPORT` (`OWNER_ID`, `LEVEL`);

      CREATE INDEX IX_PARENT_ID ON `DCP_DATA_REPORT` (`PARENT_ID`);

      ALTER TABLE `DCP_DATA_REPORT`
        ADD CONSTRAINT IX_ID_LEVEL UNIQUE (`FK_TENANT_ID`, `OWNER_ID`, `LEVEL`);

      ALTER TABLE `DCP_DATA_REPORT`
        ADD CONSTRAINT `FK_DCPDATAREPORT_FKTENANTID_TENANT` FOREIGN KEY (
        `FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;

      ALTER TABLE `MODEL_SUMMARY` ADD COLUMN `PYTHON_MAJOR_VERSION` VARCHAR(5) AFTER `PIVOT_ARTIFACT_PATH`;
      ALTER TABLE `AI_MODEL` ADD COLUMN `PYTHON_MAJOR_VERSION` VARCHAR(5) AFTER `PREDICTION_TYPE`;

      CREATE TABLE `DCP_MATCH_RULE`
        (
           `PID`                      BIGINT NOT NULL auto_increment,
           `ACCEPT_CRITERION`         JSON,
           `ALLOWED_VALUES`           JSON,
           `CREATED`                  DATETIME NOT NULL,
           `DISPLAY_NAME`             VARCHAR(255) NOT NULL,
           `EXCLUSION_CRITERION_LIST` JSON,
           `MATCH_KEY`                VARCHAR(30),
           `MATCH_RULE_ID`            VARCHAR(255) NOT NULL,
           `REVIEW_CRITERION`         JSON,
           `RULE_TYPE`                VARCHAR(30) NOT NULL,
           `SOURCE_ID`                VARCHAR(255) NOT NULL,
           `STATE`                    VARCHAR(30) NOT NULL,
           `UPDATED`                  DATETIME NOT NULL,
           `VERSION_ID`               INTEGER NOT NULL,
           `FK_TENANT_ID`             BIGINT NOT NULL,
           PRIMARY KEY (`PID`)
        )
      engine=InnoDB;

      CREATE INDEX IX_MATCH_RULE_ID ON `DCP_MATCH_RULE` (`MATCH_RULE_ID`);

      CREATE INDEX IX_SOURCE_ID ON `DCP_MATCH_RULE` (`SOURCE_ID`);

      ALTER TABLE `DCP_MATCH_RULE`
        ADD CONSTRAINT UX_MATCH_VERSION UNIQUE (`FK_TENANT_ID`, `MATCH_RULE_ID`,
        `VERSION_ID`);

      ALTER TABLE `DCP_MATCH_RULE`
        ADD CONSTRAINT `FK_DCPMATCHRULE_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`
        ) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;
        
      ALTER TABLE `LOOKUP_ID_MAP` ADD COLUMN `CONTACT_ID` VARCHAR(255);

      ALTER TABLE `DCP_UPLOAD` ADD COLUMN `CREATED_BY` varchar(255);

      ALTER TABLE `PLAY_LAUNCH` ADD COLUMN `FOLDER_ID` VARCHAR(255);

      ALTER TABLE `TENANT` ADD COLUMN `JOB_NOTIFICATION_LEVEL` JSON;

      CREATE TABLE `JOURNEY_STAGE` (
        `PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `DISPLAY_NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `PREDICATES` json DEFAULT NULL,
        `PRIORITY` int(11) NOT NULL,
        `STAGE_NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `FK_TENANT_ID` bigint(20) NOT NULL,
        PRIMARY KEY (`PID`),
        UNIQUE KEY `UKdg4hehgp0xn01bx91t7qwcf1m` (`STAGE_NAME`,`FK_TENANT_ID`),
        KEY `FK_JOURNEYSTAGE_FKTENANTID_TENANT` (`FK_TENANT_ID`),
        CONSTRAINT `FK_JOURNEYSTAGE_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
      ) ENGINE=InnoDB

      ALTER TABLE `ATLAS_S3_IMPORT_MESSAGE` CHANGE COLUMN `HOST_URL` `HOST_URL` VARCHAR(255) NULL DEFAULT NULL ;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
