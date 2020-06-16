/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script. This file DDL/DML can be applied at 'release regression' & 'release window' cycle
* Rule - Contains DDL/DML sql queries.  Should maintain backward compatibility.
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      ALTER TABLE `DCP_UPLOAD` ADD COLUMN `UPLOAD_DIAGNOSTICS` JSON;

      CREATE TABLE `DCP_DATA_REPORT`
        (
           `PID`                     BIGINT NOT NULL auto_increment,
           `BASIC_STATS`             JSON,
           `CREATED`                 DATETIME NOT NULL,
           `DATA_SNAPSHOT_TIME`      DATETIME,
           `DUPLICATION_REPORT`      JSON,
           `GEO_DISTRIBUTION_REPORT` JSON,
           `INPUT_PRESENCE_REPORT`   JSON,
           `LEVEL`                   VARCHAR(20),
           `MATCH_TO_DUNS_REPORT`    JSON,
           `OWNER_ID`                VARCHAR(255) NOT NULL,
           `PARENT_ID`               BIGINT,
           `REFRESH_TIME`            DATETIME,
           `UPDATED`                 DATETIME NOT NULL,
           `FK_TENANT_ID`            BIGINT NOT NULL,
           PRIMARY KEY (`PID`)
        )
      engine=InnoDB;

      CREATE INDEX IX_OWNER_ID_LEVEL ON `DCP_DATA_REPORT` (`OWNER_ID`, `LEVEL`);

      CREATE INDEX IX_PARENT_ID ON `DCP_DATA_REPORT` (`PARENT_ID`);

      ALTER TABLE `DCP_DATA_REPORT`
        ADD CONSTRAINT IX_ID_LEVEL UNIQUE (`FK_TENANT_ID`, `OWNER_ID`, `LEVEL`);

      ALTER TABLE `DCP_DATA_REPORT`
        ADD CONSTRAINT `FK_DCPDATAREPORT_FKTENANTID_TENANT` FOREIGN KEY (
        `FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE;



  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
