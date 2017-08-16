USE `PLS_MultiTenant`;

create table `RATING_ENGINE` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `CREATED_BY` varchar(255) not null, `DISPLAY_NAME` varchar(255), `ID` varchar(255) not null unique, `note` varchar(2048), `STATUS` varchar(255) not null, `TYPE` varchar(255) not null, `UPDATED` datetime not null, FK_SEGMENT_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `RATING_ENGINE` add index FKF5F72B24E88DD898 (FK_SEGMENT_ID), add constraint FKF5F72B24E88DD898 foreign key (FK_SEGMENT_ID) references `METADATA_SEGMENT` (`PID`) on delete cascade;
alter table `RATING_ENGINE` add index FKF5F72B2436865BC (FK_TENANT_ID), add constraint FKF5F72B2436865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;

ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK`
  ADD UNIQUE INDEX UNIQUE_ID (UNIQUE_ID),
  CHANGE COLUMN FK_DATA_ID FK_DATA_ID bigint(20) NULL AFTER FK_FEED_ID,
  CHANGE COLUMN FK_FEED_ID FK_FEED_ID bigint(20) NOT NULL AFTER UNIQUE_ID,
  CHANGE COLUMN FK_TEMPLATE_ID FK_TEMPLATE_ID bigint(20) NULL AFTER FK_DATA_ID;

ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
  ADD COLUMN `APPLICATION_ID` varchar(255) DEFAULT NULL,
  ADD COLUMN `CONTACTS_NUM` bigint,
  ADD COLUMN `ACCOUNTS_NUM` bigint;
  ADD COLUMN FK_RATING_ENGINE_ID bigint(20) NULL AFTER TIMESTAMP, 
  ADD CONSTRAINT FK25833494623258 FOREIGN KEY(FK_RATING_ENGINE_ID) REFERENCES `PLS_MultiTenant`.`RATING_ENGINE`(PID),   ADD INDEX FK25833494623258 (FK_RATING_ENGINE_ID); 

create table `RULE_BASED_MODEL` (`PID` bigint not null, `CREATED` datetime not null, `ID` varchar(255) not null unique, `Iteration` integer, `UPDATED` datetime not null, FK_RATING_ENGINE_ID bigint not null, `CASE_LOOKUP` longtext, primary key (`PID`)) ENGINE=InnoDB;
alter table `RULE_BASED_MODEL` add index FKAD89A1E79462325877cb5a9a (FK_RATING_ENGINE_ID), add constraint FKAD89A1E79462325877cb5a9a foreign key (FK_RATING_ENGINE_ID) references `RATING_ENGINE` (`PID`);

create table `AI_MODEL` (`PID` bigint not null, `CREATED` datetime not null, `ID` varchar(255) not null unique, `Iteration` integer, `UPDATED` datetime not null, FK_RATING_ENGINE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `AI_MODEL` add index FKAD89A1E7946232581c51b032 (FK_RATING_ENGINE_ID), add constraint FKAD89A1E7946232581c51b032 foreign key (FK_RATING_ENGINE_ID) references `RATING_ENGINE` (`PID`);

alter table `DATAFEED` add (`ACTIVE_PROFILE` bigint);

create table `DATAFEED_PROFILE` (
`PID` bigint not null auto_increment unique, 
`FEED_EXEC_ID` bigint not null, 
`WORKFLOW_ID` bigint, 
FK_FEED_ID bigint not null, 
primary key (`PID`)) ENGINE=InnoDB;

alter table `DATAFEED_PROFILE` add index FK8299D49299B68AE3 (FK_FEED_ID), 
add constraint FK8299D49299B68AE3 foreign key (FK_FEED_ID) references `DATAFEED` (`PID`) on delete cascade;

ALTER TABLE `PLS_MultiTenant`.`PLAY` ADD CREATED_BY varchar(255) DEFAULT "lattice@lattice-engines.com" NOT NULL,
 ADD EXCLUDE_ACCOUNTS_WITHOUT_SFID boolean NULL,
 ADD EXCLUDE_CONTACTS_WITHOUT_SFID boolean NULL;

ALTER TABLE `PLS_MultiTenant`.`OAUTH2_ACCESS_TOKEN` ADD LAST_MODIFIED_TIME BIGINT(20) DEFAULT 946656000000 NOT NULL;

CREATE TABLE `PLS_MultiTenant`.`TALKINGPOINT` (
  `PID` bigint not null auto_increment unique,
  `CONTENT` longtext,
  `CREATED` datetime not null,
  `NAME` varchar(255) not null unique,
  `OFFSET` integer,
  `TITLE` varchar(255),
  `UPDATED` datetime not null,
  `PLAY_ID` bigint not null,
  primary key (`PID`)) ENGINE=InnoDB;

ALTER TABLE `PLS_MultiTenant`.`TALKINGPOINT` add index FK85460B9AB11F603 (PLAY_ID), add constraint FK85460B9AB11F603 foreign key (PLAY_ID) references `PLAY` (`PID`) on delete cascade;

ALTER TABLE oauth2.oauth_access_token ADD CONSTRAINT UNIQUE(authentication_id);