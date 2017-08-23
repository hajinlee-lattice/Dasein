USE `Data_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DROP PROCEDURE IF EXISTS `UpdateRecommendation`;

DELIMITER //
CREATE PROCEDURE `UpdateRecommendation`()
  BEGIN
    ALTER TABLE `Recommendation`
    DROP COLUMN `CONTACTS`;

    ALTER TABLE `Recommendation`
    ADD `CONTACTS` LONGTEXT;
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    START TRANSACTION;
    CALL `UpdateRecommendation`();
    COMMIT;
  END;
//
DELIMITER ;

CALL `UpdateSchema`();



