/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB changes in production.
* Ensure to maintain backward compatibility.
*/

USE `Data_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
-- User input section (DDL/DML). This is just a template, developer can modify based on need.
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- SECTION: Lattice DB Script





      -- SECTION: DCP DB Script






  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
