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
      
      ALTER TABLE `DCP_UPLOAD` ADD COLUMN `DISPLAY_NAME` varchar(255);

      ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK`
      ADD COLUMN `SPEC_TYPE` VARCHAR(255) NULL AFTER `SUBTYPE`;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
