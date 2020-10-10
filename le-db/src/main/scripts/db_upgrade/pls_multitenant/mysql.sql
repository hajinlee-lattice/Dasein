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

      ALTER TABLE `PLS_MultiTenant`.`DCP_MATCH_RULE`
        ADD COLUMN `DATA_DOMAIN` VARCHAR(255),
        ADD COLUMN `DATA_RECORD_TYPE` VARCHAR(255);

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
          `ALERT_MESSAGE_TEMPLATE`    longtext      not null,
          `CREATED`                   datetime      not null,
          `NAME`                      varchar(255)  not null,
          `IS_ACTIVE`                 bit           not null,
          `QUALIFICATION_PERIOD_DAYS` bigint        not null,
          `UPDATED`                   datetime      not null,
          `FK_TENANT_ID`              bigint        not null,
          PRIMARY KEY (`PID`),
          UNIQUE KEY `UK_ALERT_NAME_TENANT` (`NAME`, `FK_TENANT_ID`)
      ) ENGINE = InnoDB;

      create table `DCP_ENRICHMENT_LAYOUT`
      (
          `PID`          bigint       not null auto_increment,
          `CREATED`      datetime     not null,
          `CREATED_BY`   varchar(255) not null,
          `domain`       varchar(255),
          `ELEMENTS`     JSON,
          `LAYOUT_ID`    varchar(255) not null,
          `RECORD_TYPE`  varchar(255),
          `SOURCE_ID`    varchar(255) not null,
          `TEAM_ID`      varchar(255),
          `UPDATED`      datetime     not null,
          `FK_TENANT_ID` bigint       not null,
          primary key (`PID`)
      ) engine = InnoDB;

      alter table `DCP_ENRICHMENT_LAYOUT`
          add constraint `FK_DCPENRICHMENTLAYOUT_FKTENANTID_TENANT`
              foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;

      alter table `DCP_ENRICHMENT_LAYOUT`
          add constraint UX_LAYOUT_LAYOUT_ID unique (`LAYOUT_ID`);
      alter table `DCP_ENRICHMENT_LAYOUT`
          add constraint UX_LAYOUT_SOURCE_ID unique (`SOURCE_ID`);


      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        ADD COLUMN `PREVIOUS_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID` VARCHAR(255),
        ADD COLUMN `PREVIOUS_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID` VARCHAR(255);

      UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        SET PREVIOUS_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID=CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID,
            PREVIOUS_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID=CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID
        WHERE LAUNCH_TYPE='DELTA';

      ALTER TABLE `PLS_MultiTenant`.`TRAY_CONNECTOR_TEST`
        ADD COLUMN `TEST_RESULT` VARCHAR(255);

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
