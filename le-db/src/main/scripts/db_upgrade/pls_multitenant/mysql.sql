USE `PLS_MultiTenant`;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN

  ALTER TABLE PLS_MULTITENANT.`PLAY` ADD COLUMN `FK_SEGMENT_ID` BIGINT NOT NULL;

  ALTER TABLE `PLAY` ADD CONSTRAINT `FK_PLAY_FKSEGMENTID_METADATASEGMENT` FOREIGN KEY (`FK_SEGMENT_ID`) REFERENCES `METADATA_SEGMENT` (`PID`) ON DELETE CASCADE;

  UPDATE PLAY P JOIN RATING_ENGINE RE ON P.FK_RATING_ENGINE_ID = RE.PID SET P.FK_SEGMENT_ID = RE.FK_SEGMENT_ID WHERE P.FK_SEGMENT_ID is null;

  ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK` ADD COLUMN `LAST_UPDATED` datetime not null default '1970-01-01 00:00:00'
  update `PLS_MultiTenant`.`DATAFEED_TASK` set `LAST_UPDATED` = `LAST_IMPORTED`
  ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK` MODIFY COLUMN `LAST_UPDATED` datetime not null

  ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK` ADD COLUMN `TEMPLATE_DISPLAY_NAME` varchar(255)
  ALTER TABLE `PLS_MultiTenant`.`DATAFEED_TASK` ADD COLUMN  `SUBTYPE` integer

  ALTER TABLE `PLS_MultiTenant`.`BUCKETED_SCORE_SUMMARY` ADD COLUMN `TOTAL_EXPECTED_REVENUE` DOUBLE PRECISION NULL;

  ALTER TABLE `PLS_MultiTenant`.`BUCKET_METADATA` ADD COLUMN `AVG_EXPECTED_REVENUE` DOUBLE PRECISION NULL;
  ALTER TABLE `PLS_MultiTenant`.`BUCKET_METADATA` ADD COLUMN `TOTAL_EXPECTED_REVENUE` DOUBLE PRECISION NULL;

  UPDATE `GlobalAuthentication`.`GlobalUser` SET Email = "ga_dev@lattice-engines.com" where Email = "bnguyen@lattice-engines.com"
  UPDATE `GlobalAuthentication`.`GlobalAuthentication` set Username = "ga_dev@lattice-engines.com" where Username = "bnguyen@lattice-engines.com"

  ALTER TABLE `PLS_MultiTenant`.`CDL_EXTERNAL_SYSTEM` DROP INDEX `UKqbbh3ppo2juf81q2jm0pbws5t` ,
  ADD UNIQUE INDEX `UKqbbh3ppo2juf81q2jm0pbws5t` (`TENANT_ID` ASC, `ENTITY` ASC);
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataCollectionArtifactTable`()
  BEGIN
    CREATE TABLE `PLS_MultiTenant`.`METADATA_DATA_COLLECTION_ARTIFACT` (
      `PID`              BIGINT AUTO_INCREMENT PRIMARY KEY,
      `CREATE_TIME`      BIGINT        NOT NULL,
      `NAME`             VARCHAR(256)  NULL,
      `URL`              VARCHAR(1024) NULL,
      `VERSION`          VARCHAR(128)  NOT NULL,
      `STATUS`           VARCHAR(64)   NOT NULL,
      `FK_COLLECTION_ID` BIGINT        NULL,
      `FK_TENANT_ID`     BIGINT        NULL,
      CONSTRAINT `FK_ARTIFACT_DATA_COLLECTION_PID`
      FOREIGN KEY (`FK_COLLECTION_ID`) REFERENCES `PLS_MultiTenant`.`METADATA_DATA_COLLECTION` (`PID`)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
      CONSTRAINT `FK_ARTIFACT_TENANT_PID`
      FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `PLS_MultiTenant`.`TENANT` (`TENANT_PID`)
    );
  END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateWorkflowJobTable`()
  BEGIN
    ALTER TABLE `WORKFLOW_JOB` ADD `EMR_CLUSTER_ID` varchar(20) AFTER `APPLICATION_ID`;
    alter table PLS_MultiTenant.CDL_EXTERNAL_SYSTEM add column ENTITY varchar(80) default "Account" not null;
  END;
//
DELIMITER;


CREATE PROCEDURE `CreateDropBoxTable`()
  BEGIN
  END;
//
DELIMITER;

CALL `CreateDataCollectionArtifactTable`();
CALL `UpdateWorkflowJobTable`();
