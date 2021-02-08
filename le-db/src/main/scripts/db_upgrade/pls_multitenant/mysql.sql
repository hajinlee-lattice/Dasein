/*
* script name - mysql.sql
* purpose - 'Release/Hotfix/Patch' DB upgrade script.
* SQL should be backwards compatible.
*/

-- *** DO NOT FORGET TO ADD rollback script to 'rollback.sql' file ***

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    -- User input section (DDL/DML). This is just a template, developer can modify based on need.
    
    ALTER TABLE `PLS_MultiTenant`.`METADATA_LIST_SEGMENT` ADD COLUMN `CONFIG` JSON;

    -- DCP-1838, author: lucascl@dnb.com product: D&B Connect
    ALTER TABLE `DCP_DATA_REPORT`
        ADD COLUMN `ROLLUP_STATUS` VARCHAR(255);




    -- DCP-2131 author: WuH@dnb.com product: D&B Connect
    ALTER TABLE `PLS_MultiTenant`.`DCP_ENRICHMENT_TEMPLATE`
          ADD COLUMN `ELEMENTS` JSON;



  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
