USE `PLS_MultiTenant`;

CREATE PROCEDURE `CreateDCPProjectTable`()
  BEGIN
    CREATE TABLE `DCP_PROJECT`
      (
         `PID`                  BIGINT NOT NULL auto_increment,
         `CREATED`              DATETIME NOT NULL,
         `CREATED_BY`           VARCHAR(255) NOT NULL,
         `DELETED`              BIT,
         `PROJECT_DESCRIPTION`  VARCHAR(4000),
         `PROJECT_DISPLAY_NAME` VARCHAR(255),
         `PROJECT_ID`           VARCHAR(255) NOT NULL,
         `PROJECT_TYPE`         VARCHAR(255),
         `ROOT_PATH`            VARCHAR(255),
         `UPDATED`              DATETIME NOT NULL,
         `FK_TENANT_ID`         BIGINT NOT NULL,
         PRIMARY KEY (`PID`)
      )
    engine=InnoDB;

    CREATE INDEX IX_PROJECT_ID ON `DCP_PROJECT` (`PROJECT_ID`);

    ALTER TABLE `DCP_PROJECT`
      ADD CONSTRAINT UX_PROJECT_ID UNIQUE (`FK_TENANT_ID`, `PROJECT_ID`);

    ALTER TABLE `DCP_PROJECT`
      ADD CONSTRAINT `FK_DCPPROJECT_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`)
      REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;
  END;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`DANTE_PUBLISHEDTALKINGPOINT` add column `PLAY_ID` bigint not null;

    UPDATE `PLS_MultiTenant`.`DANTE_PUBLISHEDTALKINGPOINT` T1 inner join `PLS_MultiTenant`.`PLAY` T2 on T1.PLAY_NAME = T2.NAME set T1.`PLAY_ID` = T2.`PID`;
    ALTER TABLE `DANTE_PUBLISHEDTALKINGPOINT` add constraint `FK_DANTEPUBLISHEDTALKINGPOINT_PLAYID_PLAY` foreign key (`PLAY_ID`) references `PLAY` (`PID`) on delete cascade;

    ALTER TABLE `PLS_MultiTenant`.`METADATA_SEGMENT` ADD COLUMN `COUNTS_OUTDATED` bit null default 0;
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` add column `DESTINATION_SYS_NAME` VARCHAR(255) AFTER `DESTINATION_ORG_ID`;

  END;

DELIMITER;
