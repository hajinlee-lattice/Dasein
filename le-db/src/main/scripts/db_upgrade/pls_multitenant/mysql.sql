USE `PLS_MultiTenant`;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`WORKFLOW_JOB`
        ADD COLUMN `CONFIGURATION` JSON NULL DEFAULT NULL,
        ADD COLUMN `STACK` VARCHAR(10) NULL DEFAULT NULL;

    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
        ADD COLUMN `CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID` varchar(255),
        ADD COLUMN `CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID` varchar(255);

    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL` c
            JOIN `PLS_MultiTenant`.`METADATA_TABLE` t ON c.FK_CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE = t.PID
        SET c.CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE_ID = t.NAME;

    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL` c
        JOIN `PLS_MultiTenant`.`METADATA_TABLE` t ON c.FK_CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE = t.PID
    SET c.CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE_ID = t.NAME;

    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
        ADD COLUMN `ADD_ACCOUNTS_TABLE_NAME` varchar(255),
        ADD COLUMN `ADD_CONTACTS_TABLE_NAME` varchar(255),
        ADD COLUMN `REMOVE_ACCOUNTS_TABLE_NAME` varchar(255),
        ADD COLUMN `REMOVE_CONTACTS_TABLE_NAME` varchar(255),
        ADD COLUMN `AUDIENCE_SIZE` bigint,
        ADD COLUMN `MATCHED_COUNT` bigint;

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

    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH` l
            JOIN `PLS_MultiTenant`.`METADATA_TABLE` t ON l.FK_ADD_ACCOUNTS_TABLE = t.PID
        SET l.ADD_ACCOUNTS_TABLE_NAME = t.NAME;

    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH` l
            JOIN `PLS_MultiTenant`.`METADATA_TABLE` t ON l.FK_ADD_CONTACTS_TABLE = t.PID
        SET l.ADD_CONTACTS_TABLE_NAME = t.NAME;

    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH` l
            JOIN `PLS_MultiTenant`.`METADATA_TABLE` t ON l.FK_REMOVE_ACCOUNTS_TABLE = t.PID
        SET l.REMOVE_ACCOUNTS_TABLE_NAME = t.NAME;

    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH` l
            JOIN `PLS_MultiTenant`.`METADATA_TABLE` t ON l.FK_REMOVE_CONTACTS_TABLE = t.PID
        SET l.REMOVE_CONTACTS_TABLE_NAME = t.NAME;

    ALTER TABLE `PLS_MultiTenant`.`METADATA_DATA_COLLECTION_TABLE`
        ADD COLUMN `SIGNATURE` VARCHAR(255) NULL DEFAULT NULL AFTER `ROLE`,
        ADD UNIQUE KEY `UK_COLLECTION_ROLE_VERSION_SIGNATURE` (`FK_COLLECTION_ID`, `ROLE`, `VERSION`, `SIGNATURE`);

    CREATE TABLE `PLS_MultiTenant`.`ATLAS_CATALOG`
    (
        `PID`          BIGINT                                  NOT NULL AUTO_INCREMENT,
        `NAME`         VARCHAR(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `CREATED`      DATETIME                                NOT NULL,
        `UPDATED`      DATETIME                                NOT NULL,
        `FK_TENANT_ID` bigint(20)                              NOT NULL,
        `FK_TASK_ID`   BIGINT(20)                              NOT NULL,
        PRIMARY KEY (`PID`),
        KEY `FK_TENANT_ID` (`FK_TENANT_ID`),
        KEY `FK_TASK_ID` (`FK_TASK_ID`),
        UNIQUE KEY `UK_NAME_TENANT` (`NAME`, `FK_TENANT_ID`),
        UNIQUE KEY `UK_TASK_TENANT` (`FK_TASK_ID`, `FK_TENANT_ID`),
        CONSTRAINT `CATALOG_FK_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE,
        CONSTRAINT `CATALOG_FK_DATAFEED_TASK` FOREIGN KEY (`FK_TASK_ID`) REFERENCES `DATAFEED_TASK` (`PID`) ON DELETE CASCADE
    ) engine=InnoDB;

    ALTER TABLE `PLS_MultiTenant`.`TENANT`
        ADD COLUMN `NOTIFICATION_TYPE` VARCHAR(45) NULL DEFAULT 'SINGLE_USER' AFTER `UI_VERSION`;
  END;
//
DELIMITER;

CALL `UpdatePLSTables`();
