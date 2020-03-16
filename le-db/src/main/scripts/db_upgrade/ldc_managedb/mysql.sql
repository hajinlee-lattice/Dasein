USE `LDC_ManageDB`;

SET SQL_SAFE_UPDATES = 0;

DROP PROCEDURE IF EXISTS `UpdateDataCloudVersionTable`;

DROP PROCEDURE IF EXISTS `UpdateDecisionGraphTable`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateDataCloudVersionTable`()
  BEGIN
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateDecisionGraphTable`()
  BEGIN
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `CreatePatchBookTable`()
  BEGIN
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    ALTER TABLE `SourceColumn`
        ADD COLUMN `ImportAction` varchar(1) DEFAULT 'P',
        ADD COLUMN `DefaultValue` varchar(100) DEFAULT '';

  END //
DELIMITER ;

CALL `UpdateSchema`();



