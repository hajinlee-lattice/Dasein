USE `PLS_MultiTenant`;

CREATE PROCEDURE `CreateDCPUploadTable`()
BEGIN
    create table `DCP_UPLOAD`
    (
        `PID`             bigint       not null auto_increment,
        `CREATED`         datetime     not null,
        `SOURCE_ID`       varchar(255) not null,
        `STATUS`          varchar(40)
        `UPDATED`         datetime     not null,
        `UPLOAD_CONFIG`   JSON,
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

DELIMITER;
