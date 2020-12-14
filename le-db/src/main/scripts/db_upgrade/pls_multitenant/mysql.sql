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


  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
