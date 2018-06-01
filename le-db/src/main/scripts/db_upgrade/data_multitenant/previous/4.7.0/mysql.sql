CREATE PROCEDURE `UpdateRecommendation`()
  BEGIN
    ALTER TABLE `Data_MultiTenant`.`Recommendation`
    ADD `DESTINATION_ORG_ID` varchar(255);

    ALTER TABLE `Data_MultiTenant`.`Recommendation`
    ADD `DESTINATION_SYS_TYPE` varchar(255);

    CREATE INDEX DESTINATION_ORG_ID ON `Data_MultiTenant`.`Recommendation` (`DESTINATION_ORG_ID`);
    CREATE INDEX DESTINATION_SYS_TYPE ON `Data_MultiTenant`.`Recommendation` (`DESTINATION_SYS_TYPE`);
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




