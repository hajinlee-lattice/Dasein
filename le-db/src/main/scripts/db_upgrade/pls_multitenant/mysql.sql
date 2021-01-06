/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB changes in production.
* Ensure to maintain backward compatibility.
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.


      -- DCP-1838, author: lucascl@dnb.com product: D&B Connect
      ALTER TABLE `DCP_DATA_REPORT`
          ADD COLUMN `ROLLUP_STATUS` VARCHAR(255);



      ALTER TABLE `PLS_MultiTenant`.`EXPORT_FIELD_METADATA_DEFAULTS`
               DROP `HISTORY_ENABLED`;

      create table `DATA_OPERATION`
          (
              `PID`             bigint       not null auto_increment,
              `DROP_PATH`       varchar(255),
              `OPERATION_TYPE`  varchar(40),
              `CONFIGURATION`   JSON,
              `CREATED`     datetime,
              `UPDATED`     datetime,
              `FK_TENANT_ID`    bigint       not null,
              primary key (`PID`)
          ) engine = InnoDB;

      ALTER TABLE `DATA_OPERATION`
              ADD CONSTRAINT `FK_DATAOPERATION_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`)
                  REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;
      CREATE INDEX IX_DROP_PATH ON `DATA_OPERATION` (`DROP_PATH`);

      ALTER TABLE `PLS_MultiTenant`.`ACTIVITY_METRIC_GROUP`
              ADD COLUMN `CSOverwrite` JSON NULL DEFAULT NULL;


  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
