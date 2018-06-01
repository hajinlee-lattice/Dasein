CREATE PROCEDURE `UpdateRecommendation`()
  BEGIN
    ALTER TABLE `Data_MultiTenant`.`Recommendation`
    ADD `DELETED` bit null DEFAULT 0;

    CREATE INDEX REC_DELETED ON `Data_MultiTenant`.`Recommendation` (`DELETED`);

    UPDATE `Data_MultiTenant`.`Recommendation`
    SET `DELETED` = 0;
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



