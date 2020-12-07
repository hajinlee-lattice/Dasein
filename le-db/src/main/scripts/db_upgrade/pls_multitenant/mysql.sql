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
      CREATE TABLE `DCP_ENRICHMENT_TEMPLATE` (
        `PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `TEMPLATE_ID` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `DOMAIN` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `RECORD_TYPE` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `ARCHIVED` boolean NOT NULL,
        `CREATE_TIME` datetime NOT NULL,
        `UPDATE_TIME` datetime NOT NULL,
        `CREATED_BY` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `FK_TENANT_ID` bigint(20) NOT NULL,
        PRIMARY KEY (`PID`),
        UNIQUE KEY `UX_TEMPLATE_ID` (`FK_TENANT_ID`,`TEMPLATE_ID`),
        CONSTRAINT `FK_ENRICHMENTTEMPLATE_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
      ) ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4
        COLLATE = utf8mb4_unicode_ci;

      DROP TABLE IF EXISTS `METADATA_LIST_SEGMENT`;
      CREATE TABLE `METADATA_LIST_SEGMENT` (
        `PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `CSV_ADAPTOR` longtext DEFAULT NULL,
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

      ALTER TABLE `PLAY`
              ADD COLUMN `TAP_TYPE` VARCHAR(255),
              ADD COLUMN `TAP_ID` VARCHAR(255),
              ADD COLUMN `ACCOUNT_TEMPLATE_ID` VARCHAR(255),
              ADD COLUMN `CONTACT_TEMPLATE_ID` VARCHAR(255);
            
      ALTER TABLE `LOOKUP_ID_MAP`
              ADD COLUMN `CONFIG_VALUES` json;

      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        ADD COLUMN `MAX_CONTACTS_PER_ACCOUNT` BIGINT(20);

      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        ADD COLUMN `MAX_ENTITIES_TO_LAUNCH` BIGINT(20);

      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#BCC9DB' where STAGE_NAME='Dark';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#51C1ED' where STAGE_NAME='Aware';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#457EBA' where STAGE_NAME='Engaged';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#2A5587' where STAGE_NAME='Known Engaged';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#33BDB7' where STAGE_NAME='Opportunity';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#70BF4A' where STAGE_NAME='Closed';
      UPDATE JOURNEY_STAGE SET DISPLAY_COLOR_CODE='#3FA40F' where STAGE_NAME='Closed-Won';

      ALTER TABLE `PLS_MultiTenant`.`ATLAS_EXPORT`
		ADD COLUMN `EXPORT_CONFIG` JSON;

	  ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_MESSAGE`
            CHANGE COLUMN `FK_DROP_BOX` `FK_DROP_BOX` BIGINT(20) NULL;

	  ALTER TABLE `PLS_MultiTenant`.`EXPORT_FIELD_METADATA_DEFAULTS`
	           DROP `HISTORY_ENABLED`;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
