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

      ALTER TABLE `DCP_ENRICHMENT_LAYOUT` ADD `TEMPLATE_ID` VARCHAR(255)

      ALTER TABLE `DCP_ENRICHMENT_LAYOUT`
        ADD CONSTRAINT `FK_DCPENRICHMENTTEMPLATE_FKTEMPLATEID_ENCRICHMENTTEMPLATE`
          FOREIGN KEY (`TEMPLATE_ID`) REFERENCES `DCP_ENRICHMENT_TEMPLATE` (`TEMPLATE_ID`) ON DELETE CASCADE ;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
