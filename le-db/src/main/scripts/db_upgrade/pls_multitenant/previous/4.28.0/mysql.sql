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
      CREATE TABLE `SLA_TERM` (
        `TERM_PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `CREATED` datetime NOT NULL,
        `DELIVERY_DURATION` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `FEATURE_FLAGS` json DEFAULT NULL,
        `PREDICATES` json DEFAULT NULL,
        `SCHEDULE_CRON` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `SLATERM_TYPE` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `TERM_NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `TIME_ZONE` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `UPDATED` datetime NOT NULL,
        `VERSION` int(11) DEFAULT NULL,
        `FK_TENANT_ID` bigint(20) DEFAULT NULL,
        PRIMARY KEY (`TERM_PID`),
        UNIQUE KEY `UKgx3erw009cka0dfarq0i31kqu` (`SLATERM_TYPE`,`FK_TENANT_ID`),
        UNIQUE KEY `UKm3raq3xgiqlhy58fei5eq8n6q` (`TERM_NAME`,`FK_TENANT_ID`),
        KEY `FK_SLATERM_FKTENANTID_TENANT` (`FK_TENANT_ID`),
        CONSTRAINT `FK_SLATERM_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
      ) ENGINE=InnoDB;

      CREATE TABLE `SLA_FULFILLMENT` (
        `PID` bigint(20) NOT NULL AUTO_INCREMENT,
        `DELIVERED_TIME` bigint(20) DEFAULT NULL,
        `DELIVERY_DEADLINE` bigint(20) DEFAULT NULL,
        `EARLIEST_KICKOFF_TIME` bigint(20) DEFAULT NULL,
        `FULFILLMENT_STATUS` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
        `JOB_FAILEDTIME` json DEFAULT NULL,
        `JOB_STARTTIME` json DEFAULT NULL,
        `SLA_VERSION` int(11) DEFAULT NULL,
        `FK_PID` bigint(20) NOT NULL,
        `FK_TENANT_ID` bigint(20) DEFAULT NULL,
        `FK_TERM_PID` bigint(20) NOT NULL,
        PRIMARY KEY (`PID`),
        UNIQUE KEY `UKknoed0nvy2kvgsxihhsq57d16` (`FK_TERM_PID`,`FK_PID`),
        KEY `FK_SLAFULFILLMENT_FKPID_ACTION` (`FK_PID`),
        KEY `FK_SLAFULFILLMENT_FKTENANTID_TENANT` (`FK_TENANT_ID`),
        CONSTRAINT `FK_SLAFULFILLMENT_FKPID_ACTION` FOREIGN KEY (`FK_PID`) REFERENCES `ACTION` (`PID`) ON DELETE CASCADE,
        CONSTRAINT `FK_SLAFULFILLMENT_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE,
        CONSTRAINT `FK_SLAFULFILLMENT_FKTERMPID_SLATERM` FOREIGN KEY (`FK_TERM_PID`) REFERENCES `SLA_TERM` (`TERM_PID`) ON DELETE CASCADE
      ) ENGINE=InnoDB;

      ALTER TABLE `PLS_MultiTenant`.`DCP_UPLOAD` ADD COLUMN `DISPLAY_NAME` VARCHAR(255);

      ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK`
        ADD COLUMN `SPEC_TYPE` VARCHAR(255) NULL AFTER `SUBTYPE`;

      ALTER TABLE `PLS_MultiTenant`.`DCP_PROJECT` ADD COLUMN `TEAM_ID` VARCHAR(255);

      ALTER TABLE `PLS_MultiTenant`.`ACTIVITY_METRIC_GROUP`
        ADD COLUMN `CATEGORIZATION` JSON NULL DEFAULT NULL;

      ALTER TABLE `PLS_MultiTenant`.`JOURNEY_STAGE`
        ADD COLUMN `DESCRIPTION` varchar(255),
        ADD COLUMN `DISPLAY_COLOR_CODE` varchar(255) NOT NULL,
        ADD COLUMN `CREATED` datetime not null DEFAULT CURRENT_TIMESTAMP,
        ADD COLUMN `UPDATED` datetime not null DEFAULT CURRENT_TIMESTAMP;


      create table `ACTIVITY_ALERTS_CONFIG` (
        `PID` bigint not null auto_increment,
        `ALERT_CATEGORY` integer not null,
        `ALERT_HEADER` varchar(255) not null,
        `ALERT_MESSAGE_TEMPLATE` varchar(255) not null,
        `CREATED` datetime not null,
        `ID` varchar(255) not null,
        `IS_ACTIVE` bit not null,
        `QUALIFICATION_PERIOD_DAYS` bigint not null,
        `UPDATED` datetime not null,
        `FK_TENANT_ID` bigint not null,
        primary key (`PID`)
        ) engine=InnoDB;

      Alter table `PLS_MultiTenant`.`ACTIVITY_ALERTS_CONFIG`
      add constraint UK_ebjcmedaqco5pdcdoq476h0v unique (`ID`),
      add constraint `FK_ACTIVITYALERTSCONFIG_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;


      CREATE TABLE `PROJECT_SYSTEM_LINK`
        (
           `PID`                 BIGINT NOT NULL auto_increment,
           `FK_IMPORT_SYSTEM_ID` BIGINT NOT NULL,
           `FK_PROJECT_ID`       BIGINT NOT NULL,
           `FK_TENANT_ID`        BIGINT NOT NULL,
           PRIMARY KEY (`PID`)
        )
      engine=InnoDB;

      ALTER TABLE `PROJECT_SYSTEM_LINK`
        ADD CONSTRAINT `FK_PROJECTSYSTEMLINK_FKIMPORTSYSTEMID_ATLASS3IMPORTSYSTEM`
        FOREIGN KEY (`FK_IMPORT_SYSTEM_ID`) REFERENCES `ATLAS_S3_IMPORT_SYSTEM` (`PID`
        ) ON DELETE CASCADE;

      ALTER TABLE `PROJECT_SYSTEM_LINK`
        ADD CONSTRAINT `FK_PROJECTSYSTEMLINK_FKPROJECTID_DCPPROJECT` FOREIGN KEY (
        `FK_PROJECT_ID`) REFERENCES `DCP_PROJECT` (`PID`) ON DELETE CASCADE;

      ALTER TABLE `PROJECT_SYSTEM_LINK`
        ADD CONSTRAINT `FK_PROJECTSYSTEMLINK_FKTENANTID_TENANT` FOREIGN KEY (
        `FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;

      ALTER TABLE `PROJECT_SYSTEM_LINK`
        ADD CONSTRAINT UX_PROJECT_SYSTEM UNIQUE (`FK_IMPORT_SYSTEM_ID`,
        `FK_PROJECT_ID`);

      ALTER TABLE `PLS_MultiTenant`.`DCP_PROJECT`
        DROP FOREIGN KEY `FK_DCPPROJECT_FKIMPORTSYSTEMID_ATLASS3IMPORTSYSTEM`;

      ALTER TABLE `PLS_MultiTenant`.`DCP_PROJECT`
        CHANGE COLUMN `FK_IMPORT_SYSTEM_ID` `FK_IMPORT_SYSTEM_ID` BIGINT(20) NULL,
        DROP INDEX `FK_DCPPROJECT_FKIMPORTSYSTEMID_ATLASS3IMPORTSYSTEM`;

      ALTER TABLE `PLS_MultiTenant`.`DCP_PROJECT` CHANGE COLUMN `DELETED` `DELETED` BIT NOT NULL;

      ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
        ADD COLUMN `READY_FOR_ROLLUP` BIT NOT NULL;

      ALTER TABLE `PLS_MultiTenant`.`DCP_UPLOAD` ADD COLUMN `DROP_FILE_TIME` DATETIME;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
