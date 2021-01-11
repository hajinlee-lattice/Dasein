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
      ALTER TABLE `PLS_MultiTenant`.`EXPORT_FIELD_METADATA_DEFAULTS`
               DROP `HISTORY_ENABLED`;

      create table `DATA_OPERATION`
          (
              `PID`             bigint       not null auto_increment,
              `DROP_PATH`       varchar(255),
              `OPERATION_TYPE`  varchar(40),
              `CONFIGURATION`   JSON,
              `CREATED`     datetime,
              `UPDATED`     datetime,
              `FK_TENANT_ID`    bigint       not null,
              primary key (`PID`)
          ) engine = InnoDB;

      ALTER TABLE `DATA_OPERATION`
              ADD CONSTRAINT `FK_DATAOPERATION_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`)
                  REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;
      CREATE INDEX IX_DROP_PATH ON `DATA_OPERATION` (`DROP_PATH`);

      ALTER TABLE `PLS_MultiTenant`.`ACTIVITY_METRIC_GROUP`
              ADD COLUMN `CSOverwrite` JSON NULL DEFAULT NULL;

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

      ALTER TABLE `PLS_MultiTenant`.`EXPORT_FIELD_METADATA_DEFAULTS`
              ADD `FORCE_POPULATE` bit not null;
      

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
