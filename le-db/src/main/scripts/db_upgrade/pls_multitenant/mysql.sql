USE `PLS_MultiTenant`;

CREATE PROCEDURE `CreateDCPUploadTable`()
BEGIN
    create table `DCP_UPLOAD`
    (
        `PID`             bigint       not null auto_increment,
        `UPLOAD_ID`       varchar(255) not null,
        `CREATED`         datetime     not null,
        `SOURCE_ID`       varchar(255) not null,
        `STATUS`          varchar(40)
        `UPDATED`         datetime     not null,
        `UPLOAD_CONFIG`   JSON,
        `FK_MATCH_CANDIDATES` bigint,
        `FK_MATCH_RESULT` bigint,
        `FK_TENANT_ID`    bigint       not null,
        primary key (`PID`)
    ) engine = InnoDB;

    create table `DCP_UPLOAD_STATISTICS`
    (
        `PID`          bigint not null auto_increment,
        `IS_LATEST`    bit,
        `STATISTICS`   JSON,
        `WORKFLOW_PID` bigint,
        `FK_UPLOAD_ID` bigint not null,
        primary key (`PID`)
    ) engine = InnoDB;

    CREATE TABLE `TIME_LINE` (
      `PID` bigint(20) NOT NULL AUTO_INCREMENT,
      `TIMELINE_ID` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `ENTITY` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
      `STREAM_TYPES` json DEFAULT NULL,
      `STREAM_IDS` json DEFAULT NULL,
      `EVENT_MAPPINGS` json DEFAULT NULL,
      `FK_TENANT_ID` bigint(20) NOT NULL,
      PRIMARY KEY (`PID`),
      UNIQUE KEY `PID_UNIQUE` (`PID`),
      UNIQUE KEY `UK_8wcodp0r5piad3n9lj9gl5yk0` (`TIMELINE_ID`),
      KEY `FK_TIMELINE_FKTENANTID_TENANT` (`FK_TENANT_ID`),
      CONSTRAINT `FK_TIMELINE_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

    ALTER TABLE `PLS_MultiTenant`.`ATLAS_STREAM`
    ADD COLUMN `STREAM_TYPE` VARCHAR(255) NULL AFTER `STREAM_ID`;

    CREATE INDEX IX_SOURCE_ID ON `DCP_UPLOAD` (`SOURCE_ID`);

    ALTER TABLE `DCP_UPLOAD`
        ADD CONSTRAINT `FK_DCPUPLOAD_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`)
            REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;
END;

CREATE PROCEDURE `UpdateImportMessageTable`()
BEGIN
    ALTER TABLE `ATLAS_S3_IMPORT_MESSAGE`
        ADD COLUMN `MESSAGE_TYPE`             VARCHAR(25),
        CHANGE COLUMN `FEED_TYPE` `FEED_TYPE` VARCHAR(255) NULL,
        CHANGE COLUMN `KEY` `KEY`             VARCHAR(500) NOT NULL;
END;

CREATE PROCEDURE `UpdatePLSTables`()
BEGIN
    ALTER TABLE `METADATA_SEGMENT`
        ADD COLUMN `TEAM_ID` VARCHAR(255);
END;

CREATE PRODEDURE `CreateFileDownloadTable`()
  BEGIN
    create table `FILE_DOWNLOAD`
      (
         `PID` bigint not null auto_increment,
         `CREATION` bigint not null,
         `FILE_DOWNLOAD_CONFIG` JSON,
         `TOKEN` varchar(255) not null,
         `TTL` integer not null,
         `FK_TENANT_ID` bigint not null,
          primary key (`PID`)
     )
    engine=InnoDB;
    alter table `FILE_DOWNLOAD` add constraint `FK_FILEDOWNLOAD_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;
  END;

CREATE PROCEDURE `UpdateDCPProjectTable`()
BEGIN
    ALTER TABLE `DCP_PROJECT`
        ADD COLUMN `RECIPIENT_LIST` JSON;
END;


Create PROCEDURE `UpdateTenantTable`()
  BEGIN
      ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `SUBSCRIBER_NUMBER` varchar(255);
  END;
  
CREATE PROCEDURE `UpdatePlayLaunchTable`()
BEGIN
    ALTER TABLE `PLAY_LAUNCH`
        ADD COLUMN `ACCOUNTS_ADDED` bigint(20) DEFAULT NULL, 
        ADD COLUMN `ACCOUNTS_DELETED` bigint(20) DEFAULT NULL, 
        ADD COLUMN `CONTACTS_ADDED` bigint(20) DEFAULT NULL,
        ADD COLUMN `CONTACTS_DELETED` bigint(20) DEFAULT NULL;
        update PLAY_LAUNCH  set ACCOUNTS_ADDED = ACCOUNTS_LAUNCHED, CONTACTS_ADDED = CONTACTS_LAUNCHED where State != 'Failed'
END;

CREATE TABLE `MODEL_FEATURE_IMPORTANCE` (
  `PID` bigint(20) NOT NULL AUTO_INCREMENT,
  `COLUMN_NAME` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `DISPLAY_NAME` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `FEATURE_IMPORTANCE` double NOT NULL,
  `FK_MODELSUMMARY_ID` bigint(20) NOT NULL,
  PRIMARY KEY (`PID`),
  KEY `FK_MODELFEATUREIMPORTANCE_FKMODELSUMMARYID_MODELSUMMARY` (`FK_MODELSUMMARY_ID`),
  CONSTRAINT `FK_MODELFEATUREIMPORTANCE_FKMODELSUMMARYID_MODELSUMMARY` FOREIGN KEY (`FK_MODELSUMMARY_ID`) REFERENCES `MODEL_SUMMARY` (`PID`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=151 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DELIMITER;
