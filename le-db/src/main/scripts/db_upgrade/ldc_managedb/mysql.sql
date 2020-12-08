/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB changes in production.
* Ensure to maintain backward compatibility.
*/

USE `LDC_ManageDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.

      -- DCP-2146 for D&B Connect by lucascl@dnb.com
      ALTER TABLE `PrimeColumn` ADD COLUMN `Example` VARCHAR (4000) NULL;


  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
