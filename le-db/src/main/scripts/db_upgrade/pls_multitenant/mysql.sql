/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB changes in production.
* Ensure to maintain backward compatibility.
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      CREATE TABLE `METADATA_LIST_SEGMENT` (
        `PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `CSV_ADAPTOR` json DEFAULT NULL,
        `DATA_TEMPLATES` json DEFAULT NULL,
        `EXTERNAL_SYSTEM_ID` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `EXTERNAL_SYSTEM` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `S3_DROP_FOLDER` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `TENANT_ID` bigint(20) NOT NULL,
        `FK_SEGMENT_ID` bigint(20) NOT NULL,
        PRIMARY KEY (`PID`)
      ) ENGINE=InnoDB;

      ALTER TABLE `METADATA_LIST_SEGMENT` ADD CONSTRAINT `FK_METADATALISTSEGMENT_FKSEGMENTID_METADATASEGMENT`
              FOREIGN KEY (`FK_SEGMENT_ID`) REFERENCES `METADATA_SEGMENT` (`PID`) ON DELETE CASCADE;

      ALTER TABLE `METADATA_SEGMENT`
              ADD COLUMN `TYPE` VARCHAR(255);

      -- author: lucascl@dnb.com, JIRA: DCP-1838, Product: D&B Connect
      ALTER TABLE `DCP_DATA_REPORT`
              ADD COLUMN `ROLLUP_STATUS` VARCHAR(255);

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
