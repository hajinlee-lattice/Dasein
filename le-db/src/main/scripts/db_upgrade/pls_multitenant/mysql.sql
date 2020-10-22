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

ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
          DROP FOREIGN KEY `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`;

ALTER TABLE `PLS_MultiTenant`.`DCP_DATA_REPORT`
ADD CONSTRAINT `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE` FOREIGN KEY (`FK_DUNS_COUNT`)
REFERENCES `PLS_MultiTenant`.`METADATA_TABLE` (`PID`) ON DELETE SET NULL;
      
  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
