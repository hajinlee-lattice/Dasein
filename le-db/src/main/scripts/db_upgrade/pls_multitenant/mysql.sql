USE `PLS_MultiTenant`;

CREATE PROCEDURE `UpdatePLSTables`()
BEGIN

    CREATE TABLE `ATLAS_STREAM`
    (
        `PID`               bigint(20)                              NOT NULL AUTO_INCREMENT,
        `AGGR_ENTITIES`     json                                    NOT NULL,
        `ATTRIBUTE_DERIVER` json    DEFAULT NULL,
        `CREATED`           datetime                                NOT NULL,
        `DATE_ATTRIBUTE`    varchar(50) COLLATE utf8mb4_unicode_ci  NOT NULL,
        `MATCH_ENTITIES`    json                                    NOT NULL,
        `NAME`              varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `PERIODS`           json                                    NOT NULL,
        `RETENTION_DAYS`    int(11) DEFAULT NULL,
        `STREAM_ID`         varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
        `UPDATED`           datetime                                NOT NULL,
        `FK_TASK_ID`        bigint(20)                              NOT NULL,
        `FK_TENANT_ID`      bigint(20)                              NOT NULL,
        PRIMARY KEY (`PID`),
        UNIQUE KEY `UKlp9f8nse09hyel7myetcxst50` (`NAME`, `FK_TENANT_ID`),
        UNIQUE KEY `UK8qhd4n6ljqg2r3cv58ln56wdt` (`STREAM_ID`, `FK_TENANT_ID`),
        KEY `FK_ATLASSTREAM_FKTASKID_DATAFEEDTASK` (`FK_TASK_ID`),
        KEY `FK_ATLASSTREAM_FKTENANTID_TENANT` (`FK_TENANT_ID`),
        CONSTRAINT `FK_ATLASSTREAM_FKTASKID_DATAFEEDTASK` FOREIGN KEY (`FK_TASK_ID`) REFERENCES `DATAFEED_TASK` (`PID`) ON DELETE CASCADE,
        CONSTRAINT `FK_ATLASSTREAM_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
    ) ENGINE = InnoDB
      DEFAULT CHARSET = utf8mb4
      COLLATE = utf8mb4_unicode_ci;

    CREATE TABLE `ATLAS_STREAM_DIMENSION`
    (
        `PID`           bigint(20)                              NOT NULL AUTO_INCREMENT,
        `CALCULATOR`    json                                    NOT NULL,
        `CREATED`       datetime                                NOT NULL,
        `DISPLAY_NAME`  varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL,
        `GENERATOR`     json                                    NOT NULL,
        `NAME`          varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
        `UPDATED`       datetime                                NOT NULL,
        `USAGES`        json                                    NOT NULL,
        `FK_CATALOG_ID` bigint(20) DEFAULT NULL,
        `FK_STREAM_ID`  bigint(20)                              NOT NULL,
        `FK_TENANT_ID`  bigint(20)                              NOT NULL,
        PRIMARY KEY (`PID`),
        UNIQUE KEY `UK1sg01cuqrjubjct3t5sdtxir0` (`NAME`, `FK_STREAM_ID`, `FK_TENANT_ID`),
        KEY `FK_ATLASSTREAMDIMENSION_FKCATALOGID_ATLASCATALOG` (`FK_CATALOG_ID`),
        KEY `FK_ATLASSTREAMDIMENSION_FKSTREAMID_ATLASSTREAM` (`FK_STREAM_ID`),
        KEY `FK_ATLASSTREAMDIMENSION_FKTENANTID_TENANT` (`FK_TENANT_ID`),
        CONSTRAINT `FK_ATLASSTREAMDIMENSION_FKCATALOGID_ATLASCATALOG` FOREIGN KEY (`FK_CATALOG_ID`) REFERENCES `ATLAS_CATALOG` (`PID`) ON DELETE CASCADE,
        CONSTRAINT `FK_ATLASSTREAMDIMENSION_FKSTREAMID_ATLASSTREAM` FOREIGN KEY (`FK_STREAM_ID`) REFERENCES `ATLAS_STREAM` (`PID`) ON DELETE CASCADE,
        CONSTRAINT `FK_ATLASSTREAMDIMENSION_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
    ) ENGINE = InnoDB
      DEFAULT CHARSET = utf8mb4
      COLLATE = utf8mb4_unicode_ci;

    CREATE TABLE `ACTIVITY_METRIC_GROUP` (
      `PID` bigint(20) NOT NULL AUTO_INCREMENT,
      `TIME_ROLLUP` json NOT NULL,
      `AGGREGATION` json NOT NULL,
      `CATEGORY` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `DESCRIPTION_TMPL` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
      `DISPLAY_NAME_TMPL` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `ENTITY` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `GROUP_ID` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `GROUP_NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `JAVA_CLASS` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `ROLLUP_DIMENSIONS` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `SUBCATEGORY_TMPL` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `FK_STREAM_ID` bigint(20) NOT NULL,
      `FK_TENANT_ID` bigint(20) NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE KEY `UKbl9ehqoffwh3vxhbkhaq9n27w` (`GROUP_ID`,`FK_TENANT_ID`),
      KEY `FK_ACTIVITYMETRICGROUP_FKSTREAMID_ATLASSTREAM` (`FK_STREAM_ID`),
      KEY `FK_ACTIVITYMETRICGROUP_FKTENANTID_TENANT` (`FK_TENANT_ID`),
      CONSTRAINT `FK_ACTIVITYMETRICGROUP_FKSTREAMID_ATLASSTREAM` FOREIGN KEY (`FK_STREAM_ID`) REFERENCES `ATLAS_STREAM` (`PID`) ON DELETE CASCADE,
      CONSTRAINT `FK_ACTIVITYMETRICGROUP_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
    );

    ALTER TABLE `PLAY_LAUNCH`
      ADD COLUMN `COMPLETE_CONTACTS_TABLE_NAME` varchar(255);

    ALTER TABLE `PLAY_LAUNCH_CHANNEL`
      MODIFY COLUMN `ALWAYS_ON` BIT DEFAULT 0 NOT NULL,
      ADD COLUMN `FK_WORKFLOW_ID` bigint,
      ADD COLUMN `RESET_DELTA_CALCULATION_DATA` BIT DEFAULT 0 NOT NULL,
      ADD COLUMN `EXPIRATION_PERIOD_STRING` varchar(255);

    -- PlayLaunchChannel PlayLaunch Columns to be deleted
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH_CHANNEL`
    DROP FOREIGN KEY FK_PLAYLAUNCHCHANNEL_FKCURRENTLAUNCHEDACCOUNTUNIVERSETABLE_METAD,
    DROP COLUMN FK_CURRENT_LAUNCHED_ACCOUNT_UNIVERSE_TABLE,
    DROP FOREIGN KEY FK_PLAYLAUNCHCHANNEL_FKCURRENTLAUNCHEDCONTACTUNIVERSETABLE_METAD,
    DROP COLUMN FK_CURRENT_LAUNCHED_CONTACT_UNIVERSE_TABLE;

    -- PlayLaunch Columns to be deleted
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
    DROP FOREIGN KEY FK_PLAYLAUNCH_FKADDACCOUNTSTABLE_METADATATABLE,
    DROP COLUMN FK_ADD_ACCOUNTS_TABLE,
    DROP FOREIGN KEY FK_PLAYLAUNCH_FKADDCONTACTSTABLE_METADATATABLE,
    DROP COLUMN FK_ADD_CONTACTS_TABLE,
    DROP FOREIGN KEY FK_PLAYLAUNCH_FKREMOVEACCOUNTSTABLE_METADATATABLE,
    DROP COLUMN FK_REMOVE_ACCOUNTS_TABLE,
    DROP FOREIGN KEY FK_PLAYLAUNCH_FKREMOVECONTACTSTABLE_METADATATABLE,
    DROP COLUMN FK_REMOVE_CONTACTS_TABLE;

    ALTER TABLE `DATAFEED_TASK`
        ADD COLUMN `INGESTION_BEHAVIOR` varchar(50) DEFAULT NULL AFTER `STATUS`;

    ALTER TABLE `ATLAS_CATALOG`
        ADD COLUMN `CATALOG_ID` varchar(100) NOT NULL AFTER `PID`,
        ADD COLUMN `PRIMARY_KEY_COLUMN` varchar(255) DEFAULT NULL AFTER `NAME`,
        ADD UNIQUE KEY `UK23y98pa5pxcy4f25xk51qq2w0` (`CATALOG_ID`, `FK_TENANT_ID`);

    ALTER TABLE PLS_MultiTenant.LOOKUP_ID_MAP
        ADD COLUMN PROSPECT_OWNER VARCHAR(255) DEFAULT null AFTER ACCOUNT_ID;

    ALTER TABLE `METADATA_TABLE`
        ADD COLUMN `UPDATEDBY` VARCHAR(255) DEFAULT NULL;

END;
//
DELIMITER;

CALL `UpdatePLSTables`();
