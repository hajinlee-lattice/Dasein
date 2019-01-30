USE `PLS_MultiTenant`;

CREATE PROCEDURE `Update_CDL_BUSINESS_CALENDAR`()
  BEGIN
    ALTER TABLE `CDL_BUSINESS_CALENDAR` MODIFY `LONGER_MONTH` INT(11) null;
  END;
//
DELIMITER

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
  alter table PLS_MultiTenant.ATLAS_DROPBOX add column REGION varchar(255);
  ALTER TABLE `PLS_MultiTenant`.`ACTION`
  ADD COLUMN `CANCELED` BIT(1) NULL DEFAULT b'0' COMMENT '0 Default 1 cancel' AFTER `FK_TENANT_ID`;
  ALTER TABLE `PLS_MultiTenant`.`ACTION`
  CHANGE COLUMN `CANCELED` `ACTION_STATUS` VARCHAR(20) NULL DEFAULT 'ACTIVE' ;
  ALTER TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` ADD COLUMN `EXT_SYS_NAME` varchar(255);
  create table `PLS_MultiTenant`.`EXTERNAL_SYSTEM_AUTHENTICATION` (`PID` bigint not null auto_increment, `CREATED` datetime not null, 
    `ID` varchar(255) not null, `TRAY_AUTHENTICATION_ID` varchar(255), `UPDATED` datetime not null, 
    `FK_LOOKUP_ID_MAP` bigint not null, `FK_TENANT_ID` bigint not null, primary key (`PID`)) engine=InnoDB;
  alter table `PLS_MultiTenant`.`EXTERNAL_SYSTEM_AUTHENTICATION` add constraint `FK_EXTERNALSYSTEMAUTHENTICATION_FKLOOKUPIDMAP_LOOKUPIDMAP` 
    foreign key (`FK_LOOKUP_ID_MAP`) references `PLS_MultiTenant`.`LOOKUP_ID_MAP` (`PID`) on delete cascade;
  alter table `PLS_MultiTenant`.`EXTERNAL_SYSTEM_AUTHENTICATION` add constraint `FK_EXTERNALSYSTEMAUTHENTICATION_FKTENANTID_TENANT` 
    foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;

  ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MONITORING` MODIFY `SOURCE_FILE` VARCHAR(4096);
    
  ALTER TABLE `PLS_MultiTenant`.`DATA_INTEG_STATUS_MONITORING` MODIFY `ERROR_FILE` VARCHAR(4096);
  
  create table `PLS_MultiTenant`.`PLAY_GROUP` (
    `PID` bigint not null auto_increment, 
    `CREATED` datetime not null, 
    `CREATED_BY` varchar(255) not null, 
    `DISPLAY_NAME` varchar(255) not null, 
    `ID` varchar(255) not null, 
    `TENANT_ID` bigint not null, 
    `UPDATED` datetime not null, 
    `UPDATED_BY` varchar(255) not null, 
    `FK_TENANT_ID` bigint not null, 
    primary key (`PID`)) 
    engine=InnoDB;
    
  create table `PLS_MultiTenant`.`PLAY_PLAY_GROUP` (
    `FK_PLAY_ID` bigint not null, 
    `FK_PLAY_GROUP_ID` bigint not null, 
    primary key (`FK_PLAY_ID`, `FK_PLAY_GROUP_ID`)) 
    engine=InnoDB;
    
  alter table `PLAY_GROUP` add constraint `FK_PLAYGROUP_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;
  alter table `PLAY_PLAY_GROUP` add constraint `FK_PLAYPLAYGROUP_FKPLAYGROUPID_PLAYGROUP` foreign key (`FK_PLAY_GROUP_ID`) references `PLAY_GROUP` (`PID`) on delete cascade;
  alter table `PLAY_PLAY_GROUP` add constraint `FK_PLAYPLAYGROUP_FKPLAYID_PLAY` foreign key (`FK_PLAY_ID`) references `PLAY` (`PID`) on delete cascade;
  
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
	create table `DATA_INTEG_STATUS_MONITORING` 
	(`PID` bigint not null auto_increment, 
	`CREATED_DATE` datetime not null, 
	`ENTITY_ID` varchar(255), 
	`ENTITY_NAME` varchar(255), 
	`ERROR_FILE` varchar(255), 
	`EVENT_COMPLETED_TIME` datetime, 
	`EVENT_STARTED_TIME` datetime, 
	`EVENT_SUBMITTED_TIME` datetime, 
	`EXTERNAL_SYSTEM_ID` varchar(255), 
	`OPERATION` varchar(255), 
	`SOURCE_FILE` varchar(255), 
	`STATUS` varchar(255), 
	`UPDATED_DATE` datetime not null, 
	`WORKFLOW_REQ_ID` varchar(255) not null, 
	`FK_TENANT_ID` bigint not null, 
	primary key (`PID`)) engine=InnoDB;

	create index WORKFLOW_REQ_ID on `DATA_INTEG_STATUS_MONITORING` (`WORKFLOW_REQ_ID`);

	alter table `DATA_INTEG_STATUS_MONITORING` add constraint `FK_DATAINTEGSTATUSMONITORING_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;


END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataIntegrationMessageTable`() 
  BEGIN
	create table `DATA_INTEG_STATUS_MESSAGE` 
	(`PID` bigint not null auto_increment, 
	`CREATED_DATE` datetime not null, 
	`EVENT_TIME` datetime, 
	`EVENT_TYPE` varchar(255), 
	`MESSAGE` varchar(255), 
	`MESSAGE_TYPE` varchar(255), 
	`UPDATED_DATE` datetime not null, 
	`WORKFLOW_REQ_ID` varchar(255), 
	`FK_DATA_INTEG_MONITORING_ID` bigint not null, 
	primary key (`PID`)) engine=InnoDB;

	alter table `DATA_INTEG_STATUS_MESSAGE` add constraint `FK_DATAINTEGSTATUSMESSAGE_FKDATAINTEGMONITORINGID_DATAINTEGSTATU` foreign key (`FK_DATA_INTEG_MONITORING_ID`) references `DATA_INTEG_STATUS_MONITORING` (`PID`);

END;
//
DELIMITER;

CALL `Update_CDL_BUSINESS_CALENDAR`();
CALL `CreateDataIntegrationMonitoringTable`();
CALL `CreateDataIntegrationMessageTable`();
