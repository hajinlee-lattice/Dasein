CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN

    ALTER TABLE `PLS_MultiTenant`.`BUCKET_METADATA` ADD COLUMN `RATING_ENGINE_ID` bigint;
        
    ALTER TABLE `PLS_MultiTenant`.`BUCKET_METADATA` add constraint `FK_BUCKETMETADATA_RATINGENGINEID_RATINGENGINE` foreign key (`RATING_ENGINE_ID`) references `RATING_ENGINE` (`PID`) on delete cascade;

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
