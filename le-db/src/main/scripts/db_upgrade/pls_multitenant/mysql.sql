USE `PLS_MultiTenant`;

CREATE TABLE `RATING_ENGINE` (`PID` bigint NOT NULL auto_increment UNIQUE, `CREATED` datetime NOT NULL, `CREATED_BY` VARCHAR(255) NOT NULL, `DISPLAY_NAME` VARCHAR(255), `ID` VARCHAR(255) NOT NULL UNIQUE, `note` VARCHAR(2048), `STATUS` VARCHAR(255) NOT NULL, `TYPE` VARCHAR(255) NOT NULL, `UPDATED` datetime NOT NULL, FK_SEGMENT_ID bigint NOT NULL, FK_TENANT_ID bigint NOT NULL, PRIMARY KEY(`PID`)) ENGINE=InnoDB;
ALTER TABLE `RATING_ENGINE` ADD index FKF5F72B24E88DD898 (FK_SEGMENT_ID), ADD CONSTRAINT FKF5F72B24E88DD898 FOREIGN KEY(FK_SEGMENT_ID) REFERENCES `METADATA_SEGMENT` (`PID`) ON DELETE CASCADE;
ALTER TABLE `RATING_ENGINE` ADD index FKF5F72B2436865BC (FK_TENANT_ID), ADD CONSTRAINT FKF5F72B2436865BC FOREIGN KEY(FK_TENANT_ID) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;

CREATE TABLE `RULE_BASED_MODEL` (`PID` bigint NOT NULL, `CREATED` datetime NOT NULL, `ID` VARCHAR(255) NOT NULL UNIQUE, `Iteration` INTEGER, `UPDATED` datetime NOT NULL, FK_RATING_ENGINE_ID bigint NOT NULL, `RULE` longtext, PRIMARY KEY(`PID`)) ENGINE=InnoDB;
ALTER TABLE `RULE_BASED_MODEL` ADD index FKAD89A1E79462325877cb5a9a (FK_RATING_ENGINE_ID), ADD CONSTRAINT FKAD89A1E79462325877cb5a9a FOREIGN KEY(FK_RATING_ENGINE_ID) REFERENCES `RATING_ENGINE` (`PID`);

CREATE TABLE `AI_MODEL` (`PID` bigint NOT NULL, `CREATED` datetime NOT NULL, `ID` VARCHAR(255) NOT NULL UNIQUE, `Iteration` INTEGER, `UPDATED` datetime NOT NULL, FK_RATING_ENGINE_ID bigint NOT NULL, PRIMARY KEY(`PID`)) ENGINE=InnoDB;
ALTER TABLE `AI_MODEL` ADD index FKAD89A1E7946232581c51b032 (FK_RATING_ENGINE_ID), ADD CONSTRAINT FKAD89A1E7946232581c51b032 FOREIGN KEY(FK_RATING_ENGINE_ID) REFERENCES `RATING_ENGINE` (`PID`);

ADD COLUMN FK_RATING_ENGINE_ID bigint(20) NULL AFTER TIMESTAMP, 
ADD CONSTRAINT FK25833494623258 FOREIGN KEY(FK_RATING_ENGINE_ID) REFERENCES `PLS_MultiTenant`.`RATING_ENGINE`(PID), ADD INDEX FK25833494623258 (FK_RATING_ENGINE_ID);

ALTER TABLE `PLS_MultiTenant`.`PLAY` 
    ADD CREATED DATETIME DEFAULT '1900-01-01 00:00:00' NOT NULL;
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
    ADD COLUMN `CONTACTS_LAUNCHED` bigint,
    ADD COLUMN `ACCOUNTS_LAUNCHED` bigint;
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