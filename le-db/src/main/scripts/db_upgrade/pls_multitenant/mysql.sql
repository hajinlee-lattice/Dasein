USE `PLS_MultiTenant`;
ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK`
  ADD UNIQUE INDEX UNIQUE_ID (UNIQUE_ID),
  CHANGE COLUMN FK_DATA_ID FK_DATA_ID bigint(20) NULL AFTER FK_FEED_ID,
  CHANGE COLUMN FK_FEED_ID FK_FEED_ID bigint(20) NOT NULL AFTER UNIQUE_ID,
  CHANGE COLUMN FK_TEMPLATE_ID FK_TEMPLATE_ID bigint(20) NULL AFTER FK_DATA_ID;

ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
  ADD COLUMN `APPLICATION_ID` varchar(255) DEFAULT NULL;

alter table `DATAFEED` add (`ACTIVE_PROFILE` bigint);

create table `DATAFEED_PROFILE` (
`PID` bigint not null auto_increment unique, 
`FEED_EXEC_ID` bigint not null, 
`WORKFLOW_ID` bigint, 
FK_FEED_ID bigint not null, 
primary key (`PID`)) ENGINE=InnoDB;

alter table `DATAFEED_PROFILE` add index FK8299D49299B68AE3 (FK_FEED_ID), 
add constraint FK8299D49299B68AE3 foreign key (FK_FEED_ID) references `DATAFEED` (`PID`) on delete cascade;

ALTER TABLE `PLS_MultiTenant`.`PLAY` ADD CREATED_BY varchar(255) DEFAULT "lattice@lattice-engines.com" NOT NULL;
