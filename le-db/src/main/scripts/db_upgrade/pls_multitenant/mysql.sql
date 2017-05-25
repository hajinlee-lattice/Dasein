USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
	create table `LAUNCH` (`PID` bigint not null auto_increment unique, `STATE` integer not null, `NAME` varchar(255) not null, `TABLE_NAME` varchar(255), `TIMESTAMP` datetime not null, FK_PLAY_ID bigint, primary key (`PID`)) ENGINE=InnoDB;
	create table `PLAY` (`PID` bigint not null auto_increment unique, `DESCRIPTION` varchar(255), `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, `SEGMENT_NAME` varchar(255), `TENANT_ID` bigint not null, FK_CALL_PREP_ID bigint, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
	create table `CALL_PREP` (`PID` bigint not null auto_increment unique, primary key (`PID`)) ENGINE=InnoDB;

	alter table `LAUNCH` add index FK856C17B3E57F1489 (FK_PLAY_ID), add constraint FK856C17B3E57F1489 foreign key (FK_PLAY_ID) references `PLAY` (`PID`) on delete cascade;
	alter table `PLAY` add index FK258334883752BA (FK_CALL_PREP_ID), add constraint FK258334883752BA foreign key (FK_CALL_PREP_ID) references `CALL_PREP` (`PID`) on delete cascade;
	alter table `PLAY` add index FK25833436865BC (FK_TENANT_ID), add constraint FK25833436865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;
        
        create table `METADATA_STATISTICS` (`PID` bigint not null auto_increment unique, `DATA` longblob not null, `NAME` varchar(255) not null unique, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`)) ENGINE=InnoDB;
        alter table METADATA_DATA_COLLECTION add column FK_STATISTICS_CONTAINER_ID BIGINT;
        alter table `METADATA_DATA_COLLECTION` add index FKF69DFD436816190C (FK_STATISTICS_CONTAINER_ID), add constraint FKF69DFD436816190C foreign key (FK_STATISTICS_CONTAINER_ID) references `METADATA_STATISTICS` (`PID`) on delete cascade;
        alter table `METADATA_STATISTICS` add index FK5C2E043336865BC (FK_TENANT_ID), add constraint FK5C2E043336865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;

        create table `DATAFEED_EXECUTION` (`PID` bigint not null auto_increment unique, `STATUS` varchar(255) not null, FK_FEED_ID bigint not null, primary key (`PID`));
        create table `DATAFEED_IMPORT` (`PID` bigint not null auto_increment unique, `ENTITY` varchar(255) not null, `FEED_TYPE` varchar(255), `SOURCE` varchar(255) not null, `SOURCE_CONFIG` longtext not null, `START_TIME` datetime not null, `FK_FEED_EXEC_ID` bigint not null, FK_DATA_ID bigint not null, primary key (`PID`), unique (`SOURCE`, `ENTITY`, `FK_FEED_EXEC_ID`));
        create table `DATAFEED_TASK` (`PID` bigint not null auto_increment unique, `ACTIVE_JOB` bigint not null, `ENTITY` varchar(255) not null, `FEED_TYPE` varchar(255), `LAST_IMPORTED` datetime not null, `SOURCE` varchar(255) not null, `SOURCE_CONFIG` longtext not null, `STAGING_DIR` longtext not null, `START_TIME` datetime not null, `STATUS` varchar(255) not null, `FK_FEED_ID` bigint not null, FK_DATA_ID bigint not null, FK_TEMPLATE_ID bigint not null, primary key (`PID`), unique (`SOURCE`, `ENTITY`, `FK_FEED_ID`));
        create table `DATAFEED` (`PID` bigint not null auto_increment unique, `ACTIVE_EXECUTION` bigint not null, `NAME` varchar(255) not null, `STATUS` varchar(255) not null, `TENANT_ID` bigint not null, FK_COLLECTION_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`));

        alter table `DATAFEED_EXECUTION` add index FK3E5D3BC1679FF377 (FK_FEED_ID), add constraint FK3E5D3BC1679FF377 foreign key (FK_FEED_ID) references `DATAFEED` (`PID`);
        alter table `DATAFEED_IMPORT` add index FKADAC237C56FF4425 (`FK_FEED_EXEC_ID`), add constraint FKADAC237C56FF4425 foreign key (`FK_FEED_EXEC_ID`) references `DATAFEED_EXECUTION` (`PID`);
        alter table `DATAFEED_IMPORT` add index FKADAC237C525FEA17 (FK_DATA_ID), add constraint FKADAC237C525FEA17 foreign key (FK_DATA_ID) references `METADATA_TABLE` (`PID`);
        alter table `DATAFEED_TASK` add index FK7679F31C80AFF377 (`FK_FEED_ID`), add constraint FK7679F31C80AFF377 foreign key (`FK_FEED_ID`) references `DATAFEED` (`PID`);
        alter table `DATAFEED_TASK` add index FK7679F31C525FEA17 (FK_DATA_ID), add constraint FK7679F31C525FEA17 foreign key (FK_DATA_ID) references `METADATA_TABLE` (`PID`);
        alter table `DATAFEED_TASK` add index FK7679F31C15766847 (FK_TEMPLATE_ID), add constraint FK7679F31C15766847 foreign key (FK_TEMPLATE_ID) references `METADATA_TABLE` (`PID`);
        create index IX_FEED_NAME on `DATAFEED` (`NAME`);
        alter table `DATAFEED` add index FK9950E0487BF7C1B7 (FK_COLLECTION_ID), add constraint FK9950E0487BF7C1B7 foreign key (FK_COLLECTION_ID) references `METADATA_DATA_COLLECTION` (`PID`);
        alter table `DATAFEED` add index FK9950E04836865BC (FK_TENANT_ID), add constraint FK9950E04836865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`);

        create table `MODEL_NOTES` (`PID` bigint not null auto_increment unique, `CREATED_BY_USER` varchar(255), `CREATION_TIMESTAMP` bigint, `ID` varchar(255) not null unique, `LAST_MODIFICATION_TIMESTAMP` bigint, `LAST_MODIFIED_BY_USER` varchar(255), `NOTES_CONTENTS` varchar(2048), `ORIGIN` varchar(255), `PARENT_MODEL_ID` varchar(255), MODEL_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
        create index MODEL_NOTES_ID_IDX on `MODEL_NOTES` (`ID`);
        alter table `MODEL_NOTES` add index FKE83891EB815935F7 (MODEL_ID), add constraint FKE83891EB815935F7 foreign key (MODEL_ID) references `MODEL_SUMMARY` (`PID`) on delete cascade;




     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



