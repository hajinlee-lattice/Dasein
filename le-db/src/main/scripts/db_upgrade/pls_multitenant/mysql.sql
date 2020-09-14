/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      UPDATE `PLS_MultiTenant`.`METADATA_SEGMENT`
        SET TEAM_ID='Global_Team' WHERE TEAM_ID is NULL;
      UPDATE `PLS_MultiTenant`.`RATING_ENGINE`
        SET TEAM_ID='Global_Team' WHERE TEAM_ID is NULL;

      ALTER TABLE `PLS_MultiTenant`.`DCP_PROJECT`
        ADD COLUMN `PURPOSE_OF_USE` JSON;

      ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
        DROP FOREIGN KEY `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`;

      ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
        ADD CONSTRAINT `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE` FOREIGN KEY (`FK_DUNS_COUNT`)
        REFERENCES `PLS_MultiTenant`.`METADATA_TABLE` (`PID`) ON DELETE CASCADE;

      ALTER TABLE `PLS_MultiTenant`.`EXTERNAL_SYSTEM_AUTHENTICATION`
	    MODIFY `TRAY_WORKFLOW_ENABLED` BIT NOT NULL DEFAULT 0;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
