/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
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

      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        ADD COLUMN `MAX_CONTACTS_PER_ACCOUNT` BIGINT(20);

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
