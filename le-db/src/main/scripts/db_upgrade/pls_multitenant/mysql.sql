USE `PLS_MultiTenant`;

create table `hibernate_sequences` ( `sequence_name` varchar(255),  `sequence_next_hi_value` integer ) ;

create table `RATING_ENGINE` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `CREATED_BY` varchar(255) not null, `DISPLAY_NAME` varchar(255), `ID` varchar(255) not null unique, `note` varchar(2048), `STATUS` varchar(255) not null, `TYPE` varchar(255) not null, `UPDATED` datetime not null, FK_SEGMENT_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;ALTER TABLE `RATING_ENGINE` ADD index FKF5F72B24E88DD898 (FK_SEGMENT_ID), ADD CONSTRAINT FKF5F72B24E88DD898 FOREIGN KEY(FK_SEGMENT_ID) REFERENCES `METADATA_SEGMENT` (`PID`) ON DELETE CASCADE;
alter table `RATING_ENGINE` add index FKF5F72B2436865BC (FK_TENANT_ID), add constraint FKF5F72B2436865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;

create table `RATING_MODEL` (`PID` bigint not null, `CREATED` datetime not null, `ID` varchar(255) not null unique, `Iteration` integer not null, `UPDATED` datetime not null, FK_RATING_ENGINE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `RATING_MODEL` add index FKAD89A1E794623258 (FK_RATING_ENGINE_ID), add constraint FKAD89A1E794623258 foreign key (FK_RATING_ENGINE_ID) references `RATING_ENGINE` (`PID`) on delete cascade;

create table `RULE_BASED_MODEL` (`RULE` longtext, `SELECTED_ATTRIBUTES` longtext, `PID` bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `RULE_BASED_MODEL` add index FK77CB5A9AFF57840E (`PID`), add constraint FK77CB5A9AFF57840E foreign key (`PID`) references `RATING_MODEL` (`PID`) on delete cascade;

create table `AI_MODEL` (`PID` bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `AI_MODEL` add index FK1C51B032FF57840E (`PID`), add constraint FK1C51B032FF57840E foreign key (`PID`) references `RATING_MODEL` (`PID`) on delete cascade;

DROP TABLE IF EXISTS `EAI_IMPORT_JOB_DETAIL`;
CREATE TABLE `EAI_IMPORT_JOB_DETAIL` (`PID` BIGINT NOT NULL AUTO_INCREMENT UNIQUE, `COLLECTION_IDENTIFIER` VARCHAR(255) NOT NULL, `COLLECTION_TS` DATETIME NOT NULL, `DETAILS` LONGBLOB NOT NULL, `LOAD_APPLICATION_ID` VARCHAR(255), `PROCESSED_RECORDS` INTEGER NOT NULL, `SEQUENCE_ID` BIGINT NOT NULL, `SOURCE_TYPE` VARCHAR(255) NOT NULL, `IMPORT_STATUS` VARCHAR(255) NOT NULL, `TARGET_PATH` VARCHAR(2048), PRIMARY KEY (`PID`), UNIQUE (`COLLECTION_IDENTIFIER`, `SEQUENCE_ID`)) ENGINE=InnoDB;
CREATE INDEX IX_COLLECTION_IDENTIFIER ON `EAI_IMPORT_JOB_DETAIL` (`COLLECTION_IDENTIFIER`);
CREATE INDEX IX_SEQUENCE_ID ON `EAI_IMPORT_JOB_DETAIL` (`SEQUENCE_ID`);

ALTER TABLE `PLS_MultiTenant`.`PLAY` 
    ADD CREATED DATETIME DEFAULT '1900-01-01 00:00:00' NOT NULL,
    ADD COLUMN FK_RATING_ENGINE_ID bigint(20) NULL AFTER TIMESTAMP, 
    ADD CONSTRAINT FK25833494623258 FOREIGN KEY(FK_RATING_ENGINE_ID) REFERENCES `PLS_MultiTenant`.`RATING_ENGINE`(PID), ADD INDEX FK25833494623258 (FK_RATING_ENGINE_ID);

UPDATE `PLS_MultiTenant`.`PLAY` 
    SET CREATED = TIMESTAMP 
    WHERE CREATED = '1900-01-01 00:00:00';
ALTER TABLE `PLS_MultiTenant`.`PLAY` MODIFY TIMESTAMP DATETIME;
--ALTER TABLE `PLS_MultiTenant`.`PLAY` DROP TIMESTAMP;

ALTER TABLE `PLS_MultiTenant`.`PLAY` 
    ADD UPDATED DATETIME DEFAULT '1900-01-01 00:00:00' NOT NULL;
UPDATE `PLS_MultiTenant`.`PLAY` 
    SET UPDATED = LAST_UPDATED_TIMESTAMP 
    WHERE UPDATED = '1900-01-01 00:00:00';
ALTER TABLE `PLS_MultiTenant`.`PLAY` MODIFY LAST_UPDATED_TIMESTAMP DATETIME;
--ALTER TABLE `PLS_MultiTenant`.`PLAY` DROP LAST_UPDATED_TIMESTAMP;

ALTER TABLE `PLS_MultiTenant`.`PLAY`
    ADD EXCLUDE_ITEMS_WITHOUT_SFID BOOLEAN NULL DEFAULT 0;
UPDATE `PLS_MultiTenant`.`PLAY` 
    SET EXCLUDE_ITEMS_WITHOUT_SFID = EXCLUDE_ACCOUNTS_WITHOUT_SFID
    WHERE EXCLUDE_ITEMS_WITHOUT_SFID = 0;

--ALTER TABLE `PLS_MultiTenant`.`PLAY` DROP EXCLUDE_ACCOUNTS_WITHOUT_SFID;
--ALTER TABLE `PLS_MultiTenant`.`PLAY` DROP EXCLUDE_CONTACTS_WITHOUT_SFID;

ALTER TABLE `PLS_MultiTenant`.`PLAY`
    ADD LAST_TALKING_POINT_PUBLISH_TIME DATETIME;

ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
    ADD CREATED DATETIME DEFAULT '1900-01-01 00:00:00' NOT NULL;
UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH`
    SET CREATED = CREATED_TIMESTAMP
    WHERE CREATED = '1900-01-01 00:00:00';
ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` MODIFY CREATED_TIMESTAMP DATETIME;
DROP INDEX PLAY_LAUNCH_CREATED_TIME ON `PLS_MultiTenant`.PLAY_LAUNCH;
CREATE INDEX PLAY_LAUNCH_CREATED ON `PLS_MultiTenant`.`PLAY_LAUNCH` (`CREATED`);
--ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` DROP CREATED_TIMESTAMP;

ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
    ADD UPDATED DATETIME DEFAULT '1900-01-01 00:00:00' NOT NULL;
UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH`
    SET UPDATED = LAST_UPDATED_TIMESTAMP
    WHERE UPDATED = '1900-01-01 00:00:00';
ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` MODIFY LAST_UPDATED_TIMESTAMP DATETIME DEFAULT '1900-01-01 00:00:00';
DROP INDEX PLAY_LAUNCH_LAST_UPD_TIME ON `PLS_MultiTenant`.PLAY_LAUNCH;
CREATE INDEX PLAY_LAUNCH_LAST_UPDATED ON `PLS_MultiTenant`.`PLAY_LAUNCH` (`UPDATED`);
--ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` DROP LAST_UPDATED_TIMESTAMP;

ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
    ADD COLUMN `LAUNCH_COMPLETION_PERCENT` double precision,
    ADD COLUMN `CONTACTS_LAUNCHED` bigint,
    ADD COLUMN `ACCOUNTS_ERRORED` bigint,
    ADD COLUMN `ACCOUNTS_SUPPRESSED` bigint,
    ADD COLUMN `ACCOUNTS_LAUNCHED` bigint;
UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH`
    SET LAUNCH_COMPLETION_PERCENT = 100
UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH`
    SET CONTACTS_LAUNCHED = CONTACTS_NUM
    WHERE CONTACTS_LAUNCHED IS NULL;
UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH`
    SET ACCOUNTS_LAUNCHED = ACCOUNTS_NUM
    WHERE ACCOUNTS_LAUNCHED IS NULL;
--ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` DROP ACCOUNTS_NUM;
--ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` DROP CONTACTS_NUM;

ALTER TABLE `PLS_MultiTenant`.`PLAY`
    ADD LAST_TALKING_POINT_PUBLISH_TIME DATETIME;
