/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB changes in production.
* Ensure to maintain backward compatibility.
*/

USE `DocumentDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.

      ALTER TABLE `DataUnit`
        ADD COLUMN `RetentionPolicy` VARCHAR(255) GENERATED ALWAYS AS (json_unquote(json_extract(`Document`,'$.retentionPolicy'))) VIRTUAL;
  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
