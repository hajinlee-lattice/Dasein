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
      DROP TABLE IF EXISTS `METADATA_LIST_SEGMENT`;
      CREATE TABLE `METADATA_LIST_SEGMENT` (
        `PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `CSV_ADAPTOR` json DEFAULT NULL,
        `DATA_TEMPLATES` json DEFAULT NULL,
        `EXTERNAL_SEGMENT_ID` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
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

      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#BCC9DB' where STAGE_NAME='Dark';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#51C1ED' where STAGE_NAME='Aware';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#457EBA' where STAGE_NAME='Engaged';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#2A5587' where STAGE_NAME='Known Engaged';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#33BDB7' where STAGE_NAME='Opportunity';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#70BF4A' where STAGE_NAME='Closed';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#3FA40F' where STAGE_NAME='Closed-Won';

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
