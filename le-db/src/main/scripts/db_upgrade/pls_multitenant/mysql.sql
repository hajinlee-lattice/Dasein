/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB upgrade script.
* SQL should be backwards compatible.
*/

-- *** DO NOT FORGET TO ADD rollback script to 'rollback.sql' file ***

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    -- User input section (DDL/DML). This is just a template, developer can modify based on need.
    
      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
              ADD COLUMN `RECORDS_STATS` LONGTEXT NULL DEFAULT NULL;
              
      CREATE TABLE `DCP_ENRICHMENT_TEMPLATE` (
        `PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `TEMPLATE_ID` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `DOMAIN` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `RECORD_TYPE` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `ARCHIVED` boolean NOT NULL DEFAULT 0,
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

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
