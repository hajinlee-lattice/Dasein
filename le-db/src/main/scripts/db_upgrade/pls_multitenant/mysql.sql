CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN   
    END;
//
DELIMITER ;


CREATE PROCEDURE `AddWorkflowJobUpdate`()
    BEGIN
        CREATE TABLE `WORKFLOW_JOB_UPDATE` (
            `PID` bigint(20) NOT NULL AUTO_INCREMENT,
            `LAST_UPDATE_TIME` bigint(20) NOT NULL,
            `WORKFLOW_PID` bigint(20) NOT NULL,
            PRIMARY KEY (`PID`),
            KEY `IX_WORKFLOW_PID` (`WORKFLOW_PID`)
        ) ENGINE=InnoDB;
    END
//
DELIMITER ;


DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
    BEGIN
        START TRANSACTION;
        CALL `UpdateCDLTables`();
        CALL `AddWorkflowJobUpdate`();
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
