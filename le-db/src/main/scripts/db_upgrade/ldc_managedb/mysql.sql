/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB upgrade script.
* SQL should be backwards compatible.
*/

-- *** DO NOT FORGET TO ADD rollback script to 'rollback.sql' file ***

USE `LDC_ManageDB`;

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
