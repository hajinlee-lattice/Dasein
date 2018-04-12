CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN

    ALTER TABLE `PLS_MultiTenant`.`AI_MODEL` add column `MODEL_SUMMARY` varchar(255);

    UPDATE `PLS_MultiTenant`.`AI_MODEL` a
    INNER JOIN `PLS_MultiTenant`.`MODEL_SUMMARY` m ON m.`PID` = a.`FK_MODEL_SUMMARY_ID`
    SET a.`MODEL_SUMMARY` = m.`ID`
    WHERE a.`FK_MODEL_SUMMARY_ID` IS NOT NULL;

    END;
//
DELIMITER ;


DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
    BEGIN
        START TRANSACTION;
        CALL `UpdateCDLTables`();
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
