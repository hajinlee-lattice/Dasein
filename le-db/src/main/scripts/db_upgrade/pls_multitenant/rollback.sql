/*
* script name - rollback.sql
* purpose - 'Release/Hotfix/Patch' DB rollback script.
* description - a rollback entry for each SQL at mysql.sql file
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `RollbackSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `RollbackSchema`()
  BEGIN
    -- User input section (DDL/DML). This is just a template, developer can modify based on need.


    -- DCP-1838, author: lucascl@dnb.com product: D&B Connect
    ALTER TABLE `DCP_DATA_REPORT`
        DROP COLUMN `ROLLUP_STATUS` VARCHAR(255);


  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `RollbackSchema`();
