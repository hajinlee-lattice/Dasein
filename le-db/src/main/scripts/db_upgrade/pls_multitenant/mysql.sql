USE `PLS_MultiTenant`;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `ACCOUNTS_DUPLICATED` BIGINT;
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `CONTACTS_DUPLICATED` BIGINT;

    ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MONITORING` ADD COLUMN `ERROR_MESSAGE` VARCHAR(1024);
    ALTER TABLE `PLS_MultiTenant`.`TENANT`
    ADD COLUMN `NOTIFICATION_LEVEL` VARCHAR(20) NULL DEFAULT 'ERROR' AFTER `EXPIRED_TIME`;

    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `CONTACTS_SELECTED` BIGINT;
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `CONTACTS_SUPPRESSED` BIGINT;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `ACCOUNT_SYSTEM_ID` VARCHAR(255);

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `CONTACT_SYSTEM_ID` VARCHAR(255);

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `DISPLAY_NAME` VARCHAR(255);

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `MAP_TO_LATTICE_ACCOUNT` BIT;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `MAP_TO_LATTICE_CONTACT` BIT;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_S3_IMPORT_SYSTEM`
      ADD COLUMN `PRIORITY` INTEGER NOT NULL;

    ALTER TABLE `PLS_MultiTenant`.`DATAFEED_EXECUTION`
      ADD COLUMN `RETRY_COUNT` INT(10) NULL DEFAULT 0 AFTER `FK_FEED_ID`;

    ALTER TABLE `PLAY_LAUNCH` ADD CONSTRAINT `FK_PLAYLAUNCH_FKPLAYLAUNCHCHANNELID_PLAYLAUNCHCHANNEL`
   	  FOREIGN KEY (`FK_PLAY_LAUNCH_CHANNEL_ID`) REFERENCES `PLAY_LAUNCH_CHANNEL` (`PID`) ON DELETE CASCADE;

    ALTER TABLE `PLAY_LAUNCH_CHANNEL` DROP FOREIGN KEY `FK_PLAYLAUNCHCHANNEL_FKPLAYLAUNCHID_PLAYLAUNCH`;
    
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

	alter table `EXPORT_FIELD_METADATA_MAPPING` add constraint `FK_EXPORTFIELDMETADATAMAPPING_FKLOOKUPIDMAP_LOOKUPIDMAP` foreign key (`FK_LOOKUP_ID_MAP`) references `LOOKUP_ID_MAP` (`PID`) on delete cascade;
	alter table `EXPORT_FIELD_METADATA_MAPPING` add constraint `FK_EXPORTFIELDMETADATAMAPPING_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`);
    ALTER TABLE `PLS_MultiTenant`.`DATAFEED`
    ADD COLUMN `SCHEDULING_GROUP` VARCHAR(20) NULL DEFAULT 'Default' AFTER `FK_TENANT_ID`;

	ALTER TABLE `PLS_MultiTenant`.`METADATA_SEGMENT` ADD COLUMN `UPDATED_BY` VARCHAR(255);
	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD COLUMN `UPDATED_BY` VARCHAR(255);
	ALTER TABLE `PLS_MultiTenant`.`RATING_MODEL` ADD COLUMN `UPDATED_BY` VARCHAR(255);
	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD COLUMN `DESCRIPTION` VARCHAR(255);

  END;
//
DELIMITER;

CALL `UpdatePLSTables`();
