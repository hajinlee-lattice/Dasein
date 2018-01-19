CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN   
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
