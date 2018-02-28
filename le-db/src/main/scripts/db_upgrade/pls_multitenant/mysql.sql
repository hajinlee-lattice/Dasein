CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN
	    alter table DATAFEED DROP COLUMN `ACTIVE_PROFILE`;
        drop table DATAFEED_PROFILE;
        alter table DATAFEED_EXECUTION ADD COLUMN `JOB_TYPE` varchar(255) not null DEFAULT 'PA';
        UPDATE DATAFEED_EXECUTION SET STATUS = 'Completed' where STATUS = 'ProcessAnalyzed';
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
