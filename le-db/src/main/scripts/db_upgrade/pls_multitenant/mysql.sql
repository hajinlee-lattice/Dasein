USE `PLS_MultiTenant`;

CREATE PROCEDURE `Update_SEGEMENT_EXPORT_OBJECT_TYPES`()
    BEGIN
		UPDATE PLS_MultiTenant.WORKFLOW_JOB
		SET `INPUT_CONTEXT` = JSON_REPLACE(
				`INPUT_CONTEXT`,
				'$.EXPORT_OBJECT_TYPE',
				'Enriched Accounts'
		)
		WHERE TYPE = 'segmentExportWorkflow'
		AND JSON_EXTRACT (`INPUT_CONTEXT`, '$.EXPORT_OBJECT_TYPE') = 'Accounts';

		UPDATE PLS_MultiTenant.WORKFLOW_JOB
		SET `INPUT_CONTEXT` = JSON_REPLACE(
				`INPUT_CONTEXT`,
				'$.EXPORT_OBJECT_TYPE',
				'Enriched Contacts (No Account Attributes)'
		)
		WHERE TYPE = 'segmentExportWorkflow'
		AND JSON_EXTRACT (`INPUT_CONTEXT`, '$.EXPORT_OBJECT_TYPE') = 'Contacts';

		UPDATE PLS_MultiTenant.WORKFLOW_JOB
		SET `INPUT_CONTEXT` = JSON_REPLACE(
				`INPUT_CONTEXT`,
				'$.EXPORT_OBJECT_TYPE',
				'Enriched Contacts with Account Attributes'
		)
		WHERE TYPE = 'segmentExportWorkflow'
		AND JSON_EXTRACT (`INPUT_CONTEXT`, '$.EXPORT_OBJECT_TYPE') = 'Accounts and Contacts';
    END;

CREATE PROCEDURE `Update_CDL_BUSINESS_CALENDAR`()
  BEGIN
  END;
//
DELIMITER

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
	ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `EXPIRED_TIME` bigint DEFAULT NULL;
	ALTER TABLE `PLS_MultiTenant`.`WORKFLOW_JOB`
    ADD COLUMN `ERROR_CATEGORY` VARCHAR(255) NULL DEFAULT 'UNKNOWN' AFTER `FK_TENANT_ID`;
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `FOLDER_NAME` VARCHAR(255);


    ALTER TABLE `PLS_MultiTenant`.`MODEL_SUMMARY` ADD COLUMN `NORMALIZATION_RATIO` double precision DEFAULT NULL;
    ALTER TABLE `PLS_MultiTenant`.`MODEL_SUMMARY` ADD COLUMN `AVERAGE_REVENUE_TEST_DATASET` double precision DEFAULT NULL;

    ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK` ADD COLUMN `S3_IMPORT_STATUS` VARCHAR(30);
    UPDATE `PLS_MultiTenant`.`DATAFEED_TASK` SET `S3_IMPORT_STATUS`='Active';

    ALTER TABLE `PLS_MultiTenant`.`METADATA_DATA_COLLECTION_STATUS` ADD COLUMN `CREATION_TIME` datetime not null default '1970-01-01 00:00:00';
    ALTER TABLE `PLS_MultiTenant`.`PLS_MultiTenant`.`METADATA_DATA_COLLECTION_STATUS` ADD COLUMN `UPDATE_TIME` datetime not null default '1970-01-01 00:00:00';
    drop table if exists `METADATA_DATA_COLLECTION_STATUS_HISTORY`;
    create table `PLS_MultiTenant`.`METADATA_DATA_COLLECTION_STATUS_HISTORY` (`PID` bigint not null auto_increment, `CREATION_TIME` datetime not null, `Detail` JSON, `TENANT_NAME` varchar(255) not null, `UPDATE_TIME` datetime not null, primary key (`PID`)) engine=InnoDB;


    ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MESSAGE` ADD COLUMN `FK_TENANT_ID` BIGINT NULL;
    UPDATE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MESSAGE` as MSG
	INNER JOIN `PLS_MultiTenant`.`DATA_INTEG_STATUS_MONITORING` as MON ON MSG.`FK_DATA_INTEG_MONITORING_ID` = MON.`PID`
	SET MSG.`FK_TENANT_ID` = MON.`FK_TENANT_ID`;
	ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MESSAGE` MODIFY COLUMN `FK_TENANT_ID` BIGINT NOT NULL;

    ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MESSAGE` ADD CONSTRAINT `FK_DATAINTEGSTATUSMESSAGE_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;

    ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MESSAGE` ADD COLUMN `EVENT_DETAIL` JSON;
    ALTER table PLS_MultiTenant.SOURCE_FILE add column FILE_ROWS bigint(20) DEFAULT NULL;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_DROPBOX` ADD COLUMN `ENCRYPTED_SECRET_KEY` varchar(255) DEFAULT NULL;

alter table `PLS_MultiTenant`.`METADATA_DATA_COLLECTION_STATUS_HISTORY` add column `FK_TENANT_ID` bigint(20) not null
alter table `PLS_MultiTenant`.`METADATA_DATA_COLLECTION_STATUS_HISTORY` add constraint `FK_METADATADATACOLLECTIONSTATUSHISTORY_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;
alter table `PLS_MultiTenant`.`METADATA_DATA_COLLECTION_STATUS_HISTORY` drop column `TENANT_NAME`

alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `CONTACTS_ERRORED` BIGINT;
ALTER TABLE `PLS_MultiTenant`.`SOURCE_FILE`
ADD COLUMN `PARTIAL_FILE` BIT(1) NULL DEFAULT 0 COMMENT 'The flag shows if we can auto import file after create template
. Default : 0 Using s3 to create template : 1' AFTER `FK_TENANT_ID`;

ALTER TABLE `PLS_MultiTenant`.`DATAFEED` ADD COLUMN `SCHEDULE_NOW` boolean NULL DEFAULT 0;
ALTER TABLE `PLS_MultiTenant`.`DATAFEED` ADD COLUMN `SCHEDULE_TIME` DATETIME DEFAULT NULL;
ALTER TABLE `PLS_MultiTenant`.`DATAFEED` ADD COLUMN `SCHEDULE_REQUEST` VARCHAR(4000) DEFAULT NULL;

create table `PLAY_LAUNCH_CHANNEL`
  (
    `PID` bigint not null auto_increment,
    `CREATED` datetime not null,
    `CREATED_BY` varchar(255) not null,
    `ID` varchar(255) not null,
    `ALWAYS_ON` bit,
    `TENANT_ID` bigint not null,
    `UPDATED` datetime not null,
    `UPDATED_BY` varchar(255) not null,
    `FK_LOOKUP_ID_MAP_ID` bigint not null,
    `FK_PLAY_ID` bigint not null,
    `FK_PLAY_LAUNCH_ID` bigint not null,
    `FK_TENANT_ID` bigint not null,
    primary key (`PID`)
  )
engine=InnoDB;

alter table `PLAY_LAUNCH_CHANNEL` add constraint `FK_PLAYLAUNCHCHANNEL_FKLOOKUPIDMAPID_LOOKUPIDMAP` foreign key (`FK_LOOKUP_ID_MAP_ID`) references `LOOKUP_ID_MAP` (`PID`) on delete cascade;
alter table `PLAY_LAUNCH_CHANNEL` add constraint `FK_PLAYLAUNCHCHANNEL_FKPLAYID_PLAY` foreign key (`FK_PLAY_ID`) references `PLAY` (`PID`) on delete cascade;
alter table `PLAY_LAUNCH_CHANNEL` add constraint `FK_PLAYLAUNCHCHANNEL_FKPLAYLAUNCHID_PLAYLAUNCH` foreign key (`FK_PLAY_LAUNCH_ID`) references `PLAY_LAUNCH` (`PID`) on delete cascade;
alter table `PLAY_LAUNCH_CHANNEL` add constraint `FK_PLAYLAUNCHCHANNEL_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;

  END;
//
DELIMITER;

CREATE PROCEDURE `CreateS3ImportMessageTable`()
  BEGIN
    CREATE TABLE `ATLAS_S3_IMPORT_MESSAGE`
      (
         `PID`         BIGINT NOT NULL auto_increment,
         `BUCKET`      VARCHAR(255),
         `CREATED`     DATETIME NOT NULL,
         `HOST_URL`    VARCHAR(255) NOT NULL,
         `KEY`         VARCHAR(255) NOT NULL,
         `FEED_TYPE`   VARCHAR(255) NOT NULL,
         `UPDATED`     DATETIME NOT NULL,
         `FK_DROP_BOX` BIGINT NOT NULL,
         PRIMARY KEY (`PID`)
      )
    engine=innodb;

    ALTER TABLE `ATLAS_S3_IMPORT_MESSAGE`
      ADD CONSTRAINT UX_KEY UNIQUE (`KEY`);

    ALTER TABLE `ATLAS_S3_IMPORT_MESSAGE`
      ADD CONSTRAINT `FK_ATLASS3IMPORTMESSAGE_FKDROPBOX_ATLASDROPBOX` FOREIGN KEY (
      `FK_DROP_BOX`) REFERENCES `ATLAS_DROPBOX` (`PID`) ON DELETE CASCADE;


  END;
//
DELIMITER;

CREATE PROCEDURE `CreateAtlasSchedulingTable`()
  BEGIN
    CREATE TABLE `ATLAS_SCHEDULING`
      (
         `PID`             BIGINT NOT NULL auto_increment,
         `CRON_EXPRESSION` VARCHAR(255),
         `TENANT_ID`       BIGINT NOT NULL,
         `TYPE`            VARCHAR(255) NOT NULL,
         `FK_TENANT_ID`    BIGINT NOT NULL,
         PRIMARY KEY (`PID`)
      )
    engine=InnoDB;

    ALTER TABLE `ATLAS_SCHEDULING`
      ADD CONSTRAINT UX_SCHEDULETYPE UNIQUE (`TENANT_ID`, `TYPE`);

    ALTER TABLE `ATLAS_SCHEDULING`
      ADD CONSTRAINT `FK_ATLASSCHEDULING_FKTENANTID_TENANT` FOREIGN KEY (
      `FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_SCHEDULING`
      ADD COLUMN `NEXT_FIRE_TIME` BIGINT(13) NULL DEFAULT NULL AFTER `FK_TENANT_ID`;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_SCHEDULING`
      ADD COLUMN `PREV_FIRE_TIME` BIGINT(13) NULL DEFAULT NULL AFTER `NEXT_FIRE_TIME`;

  END;
//
DELIMITER;

CREATE PROCEDURE `CreateAtlasExportTable`()
  BEGIN
    CREATE TABLE `ATLAS_EXPORT`
      (
         `PID`                     BIGINT NOT NULL auto_increment,
         `DATE_PREFIX`             VARCHAR(15),
         `EXPORT_TYPE`             VARCHAR(255),
         `FILES_UNDER_DROPFOLDER`  JSON,
         `FILES_UNDER_SYSTEM_PATH` JSON,
         `TENANT_ID`               BIGINT NOT NULL,
         `UUID`                    VARCHAR(255) NOT NULL,
         `FK_TENANT_ID`            BIGINT NOT NULL,
         PRIMARY KEY (`PID`)
      )
    engine=InnoDB;

    CREATE INDEX IX_UUID ON `ATLAS_EXPORT` (`UUID`);

    ALTER TABLE `ATLAS_EXPORT`
      ADD CONSTRAINT UX_UUID UNIQUE (`TENANT_ID`, `UUID`);

    ALTER TABLE `ATLAS_EXPORT`
      ADD CONSTRAINT `FK_ATLASEXPORT_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`)
      REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateS3ImportSystemTable`()
  BEGIN
    CREATE TABLE `ATLAS_S3_IMPORT_SYSTEM`
      (
         `PID`          BIGINT NOT NULL auto_increment,
         `NAME`         VARCHAR(255) NOT NULL,
         `SYSTEM_TYPE`  VARCHAR(30) NOT NULL,
         `TENANT_ID`    BIGINT NOT NULL,
         `FK_TENANT_ID` BIGINT NOT NULL,
         PRIMARY KEY (`PID`)
      )
    engine=InnoDB;

    CREATE INDEX IX_SYSTEM_NAME ON `ATLAS_S3_IMPORT_SYSTEM` (`NAME`);

    ALTER TABLE `ATLAS_S3_IMPORT_SYSTEM`
      ADD CONSTRAINT `UKecvbed8d0hcubasf0fthwqs2r` UNIQUE (`TENANT_ID`, `NAME`);

    ALTER TABLE `ATLAS_S3_IMPORT_SYSTEM`
      ADD CONSTRAINT `FK_ATLASS3IMPORTSYSTEM_FKTENANTID_TENANT` FOREIGN KEY (
      `FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;

  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataCollectionArtifactTable`()
  BEGIN
  END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateWorkflowJobTable`()
  BEGIN
  END;
//
DELIMITER;


CREATE PROCEDURE `CreateDropBoxTable`()
  BEGIN
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataIntegrationMonitoringTable`()
  BEGIN
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataIntegrationMessageTable`()
  BEGIN
  END;
//
DELIMITER;

CALL `Update_SEGEMENT_EXPORT_OBJECT_TYPES`();

CALL `Update_CDL_BUSINESS_CALENDAR`();
CALL `CreateDataIntegrationMonitoringTable`();
CALL `CreateDataIntegrationMessageTable`();
CALL `CreateS3ImportSystemTable`();
CALL `CreateAtlasSchedulingTable`();
CALL `CreateAtlasExportTable`();
