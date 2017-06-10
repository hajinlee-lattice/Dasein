USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
        create table `PLAY_LAUNCH` (`PID` bigint not null auto_increment unique, `CREATED_TIMESTAMP` datetime not null, `DESCRIPTION` varchar(255), `LAST_UPDATED_TIMESTAMP` datetime not null, `LAUNCH_ID` varchar(255) not null unique, `STATE` integer not null, `TABLE_NAME` varchar(255), `TENANT_ID` bigint not null, FK_PLAY_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
	create table `PLAY` (`PID` bigint not null auto_increment unique, `DESCRIPTION` varchar(255), `DISPLAY_NAME` varchar(255) not null, `LAST_UPDATED_TIMESTAMP` datetime not null, `NAME` varchar(255) not null, `SEGMENT_NAME` varchar(255), `TENANT_ID` bigint not null, `TIMESTAMP` datetime not null, FK_CALL_PREP_ID bigint, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
	create table `CALL_PREP` (`PID` bigint not null auto_increment unique, primary key (`PID`)) ENGINE=InnoDB;

        create index PLAY_LAUNCH_CREATED_TIME on `PLAY_LAUNCH` (`CREATED_TIMESTAMP`);
        create index PLAY_LAUNCH_LAST_UPD_TIME on `PLAY_LAUNCH` (`LAST_UPDATED_TIMESTAMP`);
        create index PLAY_LAUNCH_ID on `PLAY_LAUNCH` (`LAUNCH_ID`);
        create index PLAY_LAUNCH_STATE on `PLAY_LAUNCH` (`STATE`);
        alter table `PLAY_LAUNCH` add index FKF6CA4F1EE57F1489 (FK_PLAY_ID), add constraint FKF6CA4F1EE57F1489 foreign key (FK_PLAY_ID) references `PLAY` (`PID`) on delete cascade;
        alter table `PLAY_LAUNCH` add index FKF6CA4F1E36865BC (FK_TENANT_ID), add constraint FKF6CA4F1E36865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;

	alter table `PLAY` add index FK258334883752BA (FK_CALL_PREP_ID), add constraint FK258334883752BA foreign key (FK_CALL_PREP_ID) references `CALL_PREP` (`PID`) on delete cascade;
	alter table `PLAY` add index FK25833436865BC (FK_TENANT_ID), add constraint FK25833436865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;
        
        create table `METADATA_STATISTICS` (`PID` bigint not null auto_increment unique, `DATA` longblob not null, `NAME` varchar(255) not null unique, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`)) ENGINE=InnoDB;
        alter table METADATA_DATA_COLLECTION add column FK_STATISTICS_CONTAINER_ID BIGINT;
        alter table `METADATA_DATA_COLLECTION` add index FKF69DFD436816190C (FK_STATISTICS_CONTAINER_ID), add constraint FKF69DFD436816190C foreign key (FK_STATISTICS_CONTAINER_ID) references `METADATA_STATISTICS` (`PID`) on delete cascade;
        alter table `METADATA_STATISTICS` add index FK5C2E043336865BC (FK_TENANT_ID), add constraint FK5C2E043336865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;

	create table `DATAFEED_EXECUTION` (`PID` bigint not null auto_increment unique, `STATUS` varchar(255) not null, `WORKFLOW_ID` bigint, FK_FEED_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
    create table `DATAFEED_IMPORT` (`PID` bigint not null auto_increment unique, `ENTITY` varchar(255) not null, `FEED_TYPE` varchar(255), `SOURCE` varchar(255) not null, `SOURCE_CONFIG` varchar(1000) not null, `START_TIME` datetime not null, `FK_FEED_EXEC_ID` bigint not null, FK_DATA_ID bigint not null, primary key (`PID`), unique (`SOURCE`, `ENTITY`, `FEED_TYPE`, `FK_FEED_EXEC_ID`)) ENGINE=InnoDB;
	create table `DATAFEED_TASK_TABLE` (`PID` bigint not null auto_increment unique, `FK_TASK_ID` bigint not null, `FK_TABLE_ID` bigint not null, primary key (`PID`), unique (`FK_TASK_ID`, `FK_TABLE_ID`)) ENGINE=InnoDB;
	create table `DATAFEED_TASK` (`PID` bigint not null auto_increment unique, `ACTIVE_JOB` varchar(255) not null, `ENTITY` varchar(255) not null, `FEED_TYPE` varchar(255), `LAST_IMPORTED` datetime not null, `SOURCE` varchar(255) not null, `SOURCE_CONFIG` varchar(1000) not null, `START_TIME` datetime not null, `STATUS` varchar(255) not null, `FK_FEED_ID` bigint not null, FK_DATA_ID bigint, FK_TEMPLATE_ID bigint not null, primary key (`PID`), unique (`SOURCE`, `ENTITY`, `FEED_TYPE`, `FK_FEED_ID`)) ENGINE=InnoDB;
	create table `DATAFEED` (`PID` bigint not null auto_increment unique, `ACTIVE_EXECUTION` bigint not null, `NAME` varchar(255) not null, `STATUS` varchar(255) not null, `TENANT_ID` bigint not null, FK_COLLECTION_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`)) ENGINE=InnoDB;
	
	alter table `DATAFEED_EXECUTION` add index FK3E5D3BC1679FF377 (FK_FEED_ID), add constraint FK3E5D3BC1679FF377 foreign key (FK_FEED_ID) references `DATAFEED` (`PID`) on delete cascade;
	alter table `DATAFEED_IMPORT` add index FKADAC237C56FF4425 (`FK_FEED_EXEC_ID`), add constraint FKADAC237C56FF4425 foreign key (`FK_FEED_EXEC_ID`) references `DATAFEED_EXECUTION` (`PID`) on delete cascade;
	alter table `DATAFEED_IMPORT` add index FKADAC237C525FEA17 (FK_DATA_ID), add constraint FKADAC237C525FEA17 foreign key (FK_DATA_ID) references `METADATA_TABLE` (`PID`) on delete cascade;
	alter table `DATAFEED_TASK_TABLE` add index FKF200D4CBD3C51D55 (`FK_TASK_ID`), add constraint FKF200D4CBD3C51D55 foreign key (`FK_TASK_ID`) references `DATAFEED_TASK` (`PID`);
	alter table `DATAFEED_TASK_TABLE` add index FKF200D4CB68A6FB47 (`FK_TABLE_ID`), add constraint FKF200D4CB68A6FB47 foreign key (`FK_TABLE_ID`) references `METADATA_TABLE` (`PID`);
	alter table `DATAFEED_TASK` add index FK7679F31C80AFF377 (`FK_FEED_ID`), add constraint FK7679F31C80AFF377 foreign key (`FK_FEED_ID`) references `DATAFEED` (`PID`) on delete cascade;
	alter table `DATAFEED_TASK` add index FK7679F31C525FEA17 (FK_DATA_ID), add constraint FK7679F31C525FEA17 foreign key (FK_DATA_ID) references `METADATA_TABLE` (`PID`) on delete cascade;
	alter table `DATAFEED_TASK` add index FK7679F31C15766847 (FK_TEMPLATE_ID), add constraint FK7679F31C15766847 foreign key (FK_TEMPLATE_ID) references `METADATA_TABLE` (`PID`) on delete cascade;
	create index IX_FEED_NAME on `DATAFEED` (`NAME`);
	alter table `DATAFEED` add index FK9950E0487BF7C1B7 (FK_COLLECTION_ID), add constraint FK9950E0487BF7C1B7 foreign key (FK_COLLECTION_ID) references `METADATA_DATA_COLLECTION` (`PID`) on delete cascade;
	alter table `DATAFEED` add index FK9950E04836865BC (FK_TENANT_ID), add constraint FK9950E04836865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;



        create table `MODEL_NOTES` (`PID` bigint not null auto_increment unique, `CREATED_BY_USER` varchar(255), `CREATION_TIMESTAMP` bigint, `ID` varchar(255) not null unique, `LAST_MODIFICATION_TIMESTAMP` bigint, `LAST_MODIFIED_BY_USER` varchar(255), `NOTES_CONTENTS` varchar(2048), `ORIGIN` varchar(255), `PARENT_MODEL_ID` varchar(255), MODEL_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
        create index MODEL_NOTES_ID_IDX on `MODEL_NOTES` (`ID`);
        alter table `MODEL_NOTES` add index FKE83891EB815935F7 (MODEL_ID), add constraint FKE83891EB815935F7 foreign key (MODEL_ID) references `MODEL_SUMMARY` (`PID`) on delete cascade;

     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



