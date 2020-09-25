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
      UPDATE `PLS_MultiTenant`.`METADATA_SEGMENT`
      SET TEAM_ID='Global_Team'
      WHERE TEAM_ID is NULL;
      UPDATE `PLS_MultiTenant`.`RATING_ENGINE`
      SET TEAM_ID='Global_Team'
      WHERE TEAM_ID is NULL;

      ALTER TABLE `PLS_MultiTenant`.`DCP_PROJECT`
          ADD COLUMN `PURPOSE_OF_USE` JSON;

      ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
          DROP FOREIGN KEY `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`;

      ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
          ADD CONSTRAINT `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE` FOREIGN KEY (`FK_DUNS_COUNT`)
              REFERENCES `PLS_MultiTenant`.`METADATA_TABLE` (`PID`) ON DELETE CASCADE;

      ALTER TABLE `PLS_MultiTenant`.`EXTERNAL_SYSTEM_AUTHENTICATION`
          MODIFY `TRAY_WORKFLOW_ENABLED` BIT NOT NULL DEFAULT 0;

      CREATE TABLE `ACTIVITY_ALERTS_CONFIG`
      (
          `PID`                       bigint        not null auto_increment,
          `ALERT_CATEGORY`            varchar(255)  not null,
          `ALERT_HEADER`              varchar(255)  not null,
          `ALERT_MESSAGE_TEMPLATE`    varchar(1000) not null,
          `CREATED`                   datetime      not null,
          `NAME`                      varchar(255)  not null,
          `IS_ACTIVE`                 bit           not null,
          `QUALIFICATION_PERIOD_DAYS` bigint        not null,
          `UPDATED`                   datetime      not null,
          `FK_TENANT_ID`              bigint        not null,
          PRIMARY KEY (`PID`),
          UNIQUE KEY `UK_ALERT_NAME_TENANT` (`NAME`, `FK_TENANT_ID`)
      ) ENGINE = InnoDB;


      CREATE TABLE `DCP_ENRICHMENT_LAYOUT`
      (
          `PID`          bigint(20)                              NOT NULL AUTO_INCREMENT,
          `LAYOUT_ID`    varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
          `DOMAIN`       varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `RECORD_TYPE`  varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `CREATED`      datetime                                NOT NULL,
          `CREATED_BY`   varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
          `DELETED`      bit(1)                                  NOT NULL,
          `TEAM_ID`      varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `UPDATED`      datetime                                NOT NULL,
          `FK_TENANT_ID` bigint(20)                              NOT NULL,
          PRIMARY KEY (`PID`),
          UNIQUE KEY `UX_LAYOUT_ID` (`FK_TENANT_ID`,`LAYOUT_ID`),
          KEY `IX_LAYOUT_ID` (`LAYOUT_ID`),
          CONSTRAINT `FK_DCPENRICHMENT_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
      ) ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4
        COLLATE = utf8mb4_unicode_ci;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
