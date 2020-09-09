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


      alter table `PLS_MultiTenant`.`DCP_DATA_REPORT`
         drop FOREIGN KEY `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`;
      alter table `PLS_MultiTenant`.`DCP_DATA_REPORT`
	 add constraint `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`
	 foreign key (`FK_DUNS_COUNT`) references `METADATA_TABLE` (`PID`) on delete cascade;



  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
