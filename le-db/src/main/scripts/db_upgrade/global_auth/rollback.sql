/*
* script name - rollback.sql
* purpose - 'Release/Hotfix/Patch' DB rollback script.
* description - a rollback entry for each SQL at mysql.sql file
*/

USE `GlobalAuthentication`;

DROP PROCEDURE IF EXISTS `RollbackSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `RollbackSchema`()
  BEGIN
    -- User input section (DDL/DML). This is just a template, developer can modify based on need.




  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `RollbackSchema`();
