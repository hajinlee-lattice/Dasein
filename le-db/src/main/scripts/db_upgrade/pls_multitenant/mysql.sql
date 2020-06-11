/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script. This file DDL/DML can be applied at 'release regression' & 'release window' cycle
* Rule - Contains DDL/DML (create, add, modify etc.) queries.  Should maintain backward compatibility.
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.




  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
