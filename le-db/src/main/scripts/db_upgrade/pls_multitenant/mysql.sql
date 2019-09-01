USE `PLS_MultiTenant`;

CREATE PROCEDURE `CreatePlayChannels`()
BEGIN
  DECLARE finished, inner_finished BOOLEAN DEFAULT FALSE;
  DECLARE tenantId INT;
  DECLARE playId INT;
  DECLARE lookupIdMapId INT;
  DECLARE playChannelCount INT;
  DECLARE tenants TEXT;
  DECLARE curs CURSOR FOR SELECT DISTINCT FK_TENANT_ID FROM PLS_MultiTenant.PLAY_LAUNCH WHERE FK_PLAY_LAUNCH_CHANNEL_ID IS NULL;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished=TRUE;

  OPEN curs;
  big_loop: LOOP
  FETCH curs INTO tenantId;
	IF finished THEN
		CLOSE curs;
		LEAVE big_loop;
	END IF;

    INNERBLOCK: BEGIN
	DECLARE curs2 CURSOR FOR SELECT p.PID, m.PID FROM PLS_MultiTenant.PLAY p CROSS JOIN PLS_MultiTenant.LOOKUP_ID_MAP m WHERE p.FK_TENANT_ID = tenantId AND m.FK_TENANT_ID = tenantId;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET inner_finished=TRUE;
    OPEN curs2;
    inner_loop: LOOP
    FETCH curs2 INTO playId, lookupIdMapId;
		IF inner_finished THEN
			SET inner_finished = 0;
            CLOSE curs2;
            LEAVE inner_loop;
		END IF;
		SELECT COUNT(PID) INTO playChannelCount FROM PLS_MultiTenant.PLAY_LAUNCH_CHANNEL WHERE FK_PLAY_ID = playId AND FK_LOOKUP_ID_MAP_ID = lookupIdMapId;
		IF (playChannelCount < 1) THEN
			INSERT INTO `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
				(`BUCKETS_TO_LAUNCH`,`CREATED`,`CREATED_BY`,`CRON_SCHEDULE_EXPRESSION`,`ID`,`ALWAYS_ON`,`LAUNCH_TYPE`,`LAUNCH_UNSCORED`,`NEXT_SCHEDULED_LAUNCH`,`TENANT_ID`,`MAX_ACCOUNTS_TO_LAUNCH`,`UPDATED`,`UPDATED_BY`,`FK_LOOKUP_ID_MAP_ID`,`FK_PLAY_ID`,`FK_TENANT_ID`,`CHANNEL_CONFIG`,`DELETED`)
				VALUES
				('[]', NOW(), 'build-admin@lattice-engines.com',NULL,CONCAT('channel__',UUID()),0,'FULL',0,NULL,tenantId,NULL,NOW(),'build-admin@lattice-engines.com',lookupIdMapId,playId,tenantId,NULL,0);
				SET tenants = concat(tenants , tenantId, ', ');
		END IF;
	END LOOP inner_loop;
    END INNERBLOCK;
  END LOOP big_loop;
END
//
DELIMITER

CREATE PROCEDURE `AttachPlayChannelsToPlayLaunch`()
BEGIN
  DECLARE launchPid INT;
  DECLARE tenantId INT;
  DECLARE playId INT;
  DECLARE destinationOrgId VARCHAR(255);
  DECLARE finished BOOLEAN DEFAULT FALSE;

  DECLARE curs CURSOR FOR SELECT PID, FK_PLAY_ID, FK_TENANT_ID, DESTINATION_ORG_ID FROM PLS_MultiTenant.PLAY_LAUNCH WHERE FK_PLAY_LAUNCH_CHANNEL_ID IS NULL AND DELETED = 0;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished=TRUE;
  OPEN curs;
  loop_logic: LOOP
  FETCH curs INTO launchPid, playId, tenantId, destinationOrgId;
	IF finished THEN
		CLOSE curs;
		LEAVE loop_logic;
	END IF;
	UPDATE PLS_MultiTenant.PLAY_LAUNCH
	SET FK_PLAY_LAUNCH_CHANNEL_ID =
		(SELECT PID FROM PLS_MultiTenant.PLAY_LAUNCH_CHANNEL WHERE FK_TENANT_ID = tenantId AND FK_PLAY_ID = playId AND FK_LOOKUP_ID_MAP_ID = (SELECT PID FROM PLS_MultiTenant.LOOKUP_ID_MAP WHERE ORG_ID = destinationOrgId AND FK_TENANT_ID = tenantID))
	WHERE PID = launchPid;
  END LOOP loop_logic;
END
//
DELIMITER

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MONITORING` ADD COLUMN `ERROR_MESSAGE` VARCHAR(1024);
    ALTER TABLE `PLS_MultiTenant`.`TENANT`
    ADD COLUMN `NOTIFICATION_LEVEL` VARCHAR(20) NULL DEFAULT 'ERROR' AFTER `EXPIRED_TIME`;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `ACCOUNT_SYSTEM_ID` VARCHAR(255),
      ADD COLUMN `CONTACT_SYSTEM_ID` VARCHAR(255),
      ADD COLUMN `DISPLAY_NAME` VARCHAR(255),
      ADD COLUMN `MAP_TO_LATTICE_ACCOUNT` BIT,
      ADD COLUMN `MAP_TO_LATTICE_CONTACT` BIT,
      ADD COLUMN `PRIORITY` INTEGER NOT NULL;

    ALTER TABLE `PLS_MultiTenant`.`DATAFEED_EXECUTION`
      ADD COLUMN `RETRY_COUNT` INT(10) NULL DEFAULT 0 AFTER `FK_FEED_ID`;

    ALTER TABLE `PLS_MultiTenant`.`SOURCE_FILE`
      ADD COLUMN `S3_BUCKET` VARCHAR(255),
      ADD COLUMN `S3_PATH` VARCHAR(255);

    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
        ADD COLUMN `CONTACTS_SELECTED` BIGINT,
        ADD COLUMN `CONTACTS_SUPPRESSED` BIGINT,
        ADD COLUMN `ACCOUNTS_DUPLICATED` BIGINT,
        ADD COLUMN `CONTACTS_DUPLICATED` BIGINT,
   	    ADD COLUMN `CHANNEL_CONFIG` longtext,
   	    ADD COLUMN `FK_ADD_ACCOUNTS_TABLE` bigint,
   	    ADD COLUMN `FK_ADD_CONTACTS_TABLE` bigint,
   	    ADD COLUMN `FK_REMOVE_ACCOUNTS_TABLE` bigint,
   	    ADD COLUMN `FK_REMOVE_CONTACTS_TABLE` bigint,
   	    ADD CONSTRAINT `FK_PLAYLAUNCH_FKPLAYLAUNCHCHANNELID_PLAYLAUNCHCHANNEL`
           	  FOREIGN KEY (`FK_PLAY_LAUNCH_CHANNEL_ID`) REFERENCES `PLAY_LAUNCH_CHANNEL` (`PID`) ON DELETE CASCADE,
   	    ADD CONSTRAINT `FK_PLAYLAUNCH_FKADDACCOUNTSTABLE_METADATATABLE`
   	        FOREIGN KEY (`FK_ADD_ACCOUNTS_TABLE`) REFERENCES `METADATA_TABLE` (`PID`) ON DELETE CASCADE,
        ADD CONSTRAINT `FK_PLAYLAUNCH_FKADDCONTACTSTABLE_METADATATABLE`
            FOREIGN KEY (`FK_ADD_CONTACTS_TABLE`) REFERENCES `METADATA_TABLE` (`PID`) ON DELETE CASCADE,
        ADD CONSTRAINT `FK_PLAYLAUNCH_FKREMOVEACCOUNTSTABLE_METADATATABLE`
            FOREIGN KEY (`FK_REMOVE_ACCOUNTS_TABLE`) REFERENCES `METADATA_TABLE` (`PID`) ON DELETE CASCADE,
        ADD CONSTRAINT `FK_PLAYLAUNCH_FKREMOVECONTACTSTABLE_METADATATABLE`
            FOREIGN KEY (`FK_REMOVE_CONTACTS_TABLE`) REFERENCES `METADATA_TABLE` (`PID`) ON DELETE CASCADE;

    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        DROP FOREIGN KEY `FK_PLAYLAUNCHCHANNEL_FKPLAYLAUNCHID_PLAYLAUNCH`,
        ADD COLUMN `DELETED` BIT NOT NULL DEFAULT 0,
        ADD COLUMN `FK_CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE` bigint,
        ADD COLUMN `FK_CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE` bigint,
        ADD CONSTRAINT `FK_PLAYLAUNCHCHANNEL_FKCURRENTLAUNCHEDACCOUNTUNIVERSETABLE_METAD`
            FOREIGN KEY (`FK_CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE`) REFERENCES `METADATA_TABLE` (`PID`) ON DELETE CASCADE,
        ADD CONSTRAINT `FK_PLAYLAUNCHCHANNEL_FKCURRENTLAUNCHEDCONTACTUNIVERSETABLE_METAD`
            FOREIGN KEY (`FK_CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE`) REFERENCES `METADATA_TABLE` (`PID`) ON DELETE CASCADE;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `SECONDARY_ACCOUNT_IDS` JSON,
      ADD COLUMN `SECONDARY_CONTACT_IDS` JSON;

    UPDATE `PLAY_LAUNCH_CHANNEL` channel
	INNER JOIN `PLAY` play ON
		channel.`FK_PLAY_ID` = play.`PID`
	SET channel.`DELETED` = 1
	WHERE (play.`DELETED` = 1 AND channel.`PID` > 0);

    create table `EXPORT_FIELD_METADATA_DEFAULTS`
    	(`PID` bigint not null auto_increment,
    	`ATTR_NAME` varchar(255) not null,
    	`DISPLAY_NAME` varchar(255) not null,
    	`ENTITY` varchar(255) not null,
    	`EXPORT_ENABLED` bit not null,
    	`EXT_SYS_NAME` varchar(255) not null,
    	`HISTORY_ENABLED` bit not null,
    	`JAVA_CLASS` varchar(255) not null,
    	`STANDARD_FIELD` bit not null,
    	primary key (`PID`)) engine=InnoDB;

	create table `EXPORT_FIELD_METADATA_MAPPING`
		(`PID` bigint not null auto_increment,
		`CREATED` datetime not null,
		`DESTINATION_FIELD` varchar(255) not null,
		`OVERWRITE_VALUE` bit not null,
		`SOURCE_FIELD` varchar(255) not null,
		`UPDATED` datetime not null,
		`FK_LOOKUP_ID_MAP` bigint not null,
		`FK_TENANT_ID` bigint not null,
		primary key (`PID`)) engine=InnoDB;

	ALTER TABLE `EXPORT_FIELD_METADATA_MAPPING`
	    ADD CONSTRAINT `FK_EXPORTFIELDMETADATAMAPPING_FKLOOKUPIDMAP_LOOKUPIDMAP`
	        FOREIGN KEY (`FK_LOOKUP_ID_MAP`) REFERENCES `LOOKUP_ID_MAP` (`PID`) ON DELETE CASCADE,
        ADD CONSTRAINT `FK_EXPORTFIELDMETADATAMAPPING_FKTENANTID_TENANT`
            FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;


    ALTER TABLE `PLS_MultiTenant`.`DATAFEED`
        ADD COLUMN `SCHEDULING_GROUP` VARCHAR(20) NULL DEFAULT 'Default' AFTER `FK_TENANT_ID`;

	ALTER TABLE `PLS_MultiTenant`.`METADATA_SEGMENT` ADD COLUMN `UPDATED_BY` VARCHAR(255);
	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD COLUMN `UPDATED_BY` VARCHAR(255);
	ALTER TABLE `PLS_MultiTenant`.`RATING_MODEL` ADD COLUMN `UPDATED_BY` VARCHAR(255);
	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD COLUMN `DESCRIPTION` VARCHAR(255);

	ALTER TABLE `PLS_MultiTenant`.`BUCKET_METADATA`
	    ADD COLUMN `ORIG_CREATION_TIMESTAMP` BIGINT(20) NULL DEFAULT NULL AFTER `CREATION_TIMESTAMP`;

	CREATE TABLE `IMPORT_MIGRATE_TRACKING` (
      `PID` bigint(20) NOT NULL AUTO_INCREMENT,
      `REPORT` json DEFAULT NULL,
      `STATUS` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
      `FK_TENANT_ID` bigint(20) NOT NULL,
      PRIMARY KEY (`PID`),
      KEY `FK_TENANT_ID` (`FK_TENANT_ID`),
      CONSTRAINT `IMPORT_MIGRATE_TRACKING_ibfk_1` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
    ) ENGINE=InnoDB;

    CREATE TABLE `MIGRATION_TRACK` (
      `PID` bigint(20) NOT NULL AUTO_INCREMENT,
      `STATUS` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
      `VERSION` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
      `CUR_ACTIVE_TABLE_NAME` json DEFAULT NULL COMMENT 'DataCollectionTable.ROLE -> list of Table.NAME',
      `FK_IMPORT_TRACKING` bigint(20) DEFAULT NULL COMMENT 'IMPORT_MIGRATE_TRACKING.pid',
      `DETAIL` json DEFAULT NULL COMMENT 'from DATA_COLLECTION_STATUS.Detail',
      `CUBES_DATA` longblob COMMENT 'from STATISTICS.CUBES_DATA',
      `NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'from STATISTICS.NAME',
      `FK_TENANT_ID` bigint(20) NOT NULL,
      `FK_COLLECTION_ID` bigint(20) DEFAULT NULL,
      PRIMARY KEY (`PID`),
      KEY `FK_IMPORT_TRACKING` (`FK_IMPORT_TRACKING`),
      KEY `FK_TENANT_ID` (`FK_TENANT_ID`),
      KEY `FK_COLLECTION_ID` (`FK_COLLECTION_ID`),
      CONSTRAINT `MIGRATION_TRACK_ibfk_1` FOREIGN KEY (`FK_IMPORT_TRACKING`) REFERENCES `IMPORT_MIGRATE_TRACKING` (`PID`),
      CONSTRAINT `MIGRATION_TRACK_ibfk_2` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE,
      CONSTRAINT `MIGRATION_TRACK_ibfk_3` FOREIGN KEY (`FK_COLLECTION_ID`) REFERENCES `METADATA_DATA_COLLECTION` (`PID`)
    ) ENGINE=InnoDB;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_EXPORT` ADD COLUMN `APPLICATION_ID` VARCHAR(255),
		ADD COLUMN `CREATED_BY` VARCHAR(255),
		ADD COLUMN `ACCOUNT_RESTRICTION` LONGTEXT,
		ADD COLUMN `CONTACT_RESTRICTION` LONGTEXT,
		ADD COLUMN `SCHEDULED` BIT NOT NULL DEFAULT 1,
		ADD COLUMN `CLEANUP_BY` DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00',
		ADD COLUMN `STATUS` VARCHAR(255) NOT NULL DEFAULT 'COMPLETED',
		ADD COLUMN `SEGMENT_NAME` VARCHAR(255),
		ADD COLUMN `FILES_TO_DELETE` JSON,
		ADD COLUMN `CREATED` DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00',
		ADD COLUMN `UPDATED` DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00';

    CREATE TABLE `CONVERT_BATCHSTORE_INFO`
      (
         `PID`            BIGINT NOT NULL auto_increment,
         `CONVERT_DETAIL` JSON,
         `FK_TENANT_ID`   BIGINT NOT NULL,
         PRIMARY KEY (`PID`)
      )
    engine=InnoDB;

    ALTER TABLE `CONVERT_BATCHSTORE_INFO`
      ADD CONSTRAINT `FK_CONVERTBATCHSTOREINFO_FKTENANTID_TENANT` FOREIGN KEY (
      `FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;

    ALTER TABLE `PLS_MultiTenant`.`WORKFLOW_JOB`
      ADD COLUMN `STACK` VARCHAR(10) NULL DEFAULT NULL AFTER `TYPE`;
      
      
      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `AUDIENCE_SIZE` bigint;
      ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `MATCHED_COUNT` bigint;

    CREATE TABLE `ATLAS_CATALOG` (
      `PID` BIGINT NOT NULL AUTO_INCREMENT,
      `NAME` VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `CREATED` DATETIME NOT NULL,
      `UPDATED` DATETIME NOT NULL,
      `FK_TENANT_ID` bigint(20) NOT NULL,
      `FK_TASK_ID` BIGINT(20) NOT NULL,
      PRIMARY KEY (`PID`),
      KEY `FK_TENANT_ID` (`FK_TENANT_ID`),
      KEY `FK_TASK_ID` (`FK_TASK_ID`),
      UNIQUE KEY `UK_NAME_TENANT` (`NAME`,`FK_TENANT_ID`),
      CONSTRAINT `CATALOG_FK_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE,
      CONSTRAINT `CATALOG_FK_DATAFEED_TASK` FOREIGN KEY (`FK_TASK_ID`) REFERENCES `DATAFEED_TASK` (`PID`) ON DELETE CASCADE
    ) engine=InnoDB;

  END;

  CALL `CreatePlayChannels`();

  CALL `AttachPlayChannelsToPlayLaunch`();
//
DELIMITER;

CALL `UpdatePLSTables`();
