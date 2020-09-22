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
        SET TEAM_ID='Global_Team' WHERE TEAM_ID is NULL;
      UPDATE `PLS_MultiTenant`.`RATING_ENGINE`
        SET TEAM_ID='Global_Team' WHERE TEAM_ID is NULL;

      ALTER TABLE `PLS_MultiTenant`.`DCP_PROJECT`
        ADD COLUMN `PURPOSE_OF_USE` JSON;

      ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
        DROP FOREIGN KEY `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`;

      ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
        ADD CONSTRAINT `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE` FOREIGN KEY (`FK_DUNS_COUNT`)
        REFERENCES `PLS_MultiTenant`.`METADATA_TABLE` (`PID`) ON DELETE CASCADE;

      ALTER TABLE `PLS_MultiTenant`.`EXTERNAL_SYSTEM_AUTHENTICATION`
	    MODIFY `TRAY_WORKFLOW_ENABLED` BIT NOT NULL DEFAULT 0;

      create table `ACTIVITY_ALERTS_CONFIG` (
        `PID` bigint not null auto_increment,
        `ALERT_CATEGORY` varchar(255) not null,
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

      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        ADD COLUMN `PREVIOUS_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID` VARCHAR(255),
        ADD COLUMN `PREVIOUS_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID` VARCHAR(255);

      UPDATE `PLS_MultiTenant.PLAY_LAUNCH_CHANNEL`
        SET PREVIOUS_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID=CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID,
            PREVIOUS_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID=CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID
        WHERE LAUNCH_TYPE='DELTA';

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
