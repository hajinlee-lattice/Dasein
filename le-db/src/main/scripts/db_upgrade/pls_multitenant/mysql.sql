CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN
        CREATE TABLE `CDL_EXTERNAL_SYSTEM` ( 
	     `PID`          BIGINT NOT NULL auto_increment, 
	     `CRM_IDS`      VARCHAR(4000), 
	     `ERP_IDS`      VARCHAR(4000), 
	     `MAP_IDS`      VARCHAR(4000), 
	     `OTHER_IDS`    VARCHAR(4000), 
	     `TENANT_ID`    BIGINT NOT NULL, 
	     `FK_TENANT_ID` BIGINT NOT NULL, 
	     PRIMARY KEY (`PID`) 
	  ) 
	ENGINE = InnoDB; 

	ALTER TABLE `CDL_EXTERNAL_SYSTEM` 
	  ADD CONSTRAINT `UKqbbh3ppo2juf81q2jm0pbws5t` UNIQUE (`TENANT_ID`); 

	ALTER TABLE `CDL_EXTERNAL_SYSTEM` 
	  ADD CONSTRAINT `FKpohehcwu5bgh0wp5gyhvl9sm` FOREIGN KEY (`FK_TENANT_ID`) 
	  REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE; 
            
        ALTER TABLE `PLS_MultiTenant`.`AI_MODEL`
		    ADD COLUMN `FK_MODEL_SUMMARY_ID` bigint;
		    
		ALTER TABLE `PLS_MultiTenant`.`AI_MODEL` ADD CONSTRAINT `FKgenp90xodrrj475g7g7xcxoti` foreign key (`FK_MODEL_SUMMARY_ID`) references `PLS_MultiTenant`.`MODEL_SUMMARY` (`PID`);
    
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateRatingEngineTable`()
    BEGIN
        ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE`
            ADD COLUMN `ACTIVE_MODEL_PID` bigint;    
    END;
//
DELIMITER ;

CREATE PROCEDURE `MigrateRatingModelTable`()
    BEGIN
        update RATING_ENGINE inner join RATING_MODEL
        on RATING_ENGINE.pid = RATING_MODEL.FK_RATING_ENGINE_ID
        set RATING_ENGINE.ACTIVE_MODEL_PID = RATING_MODEL.PID
        where RATING_ENGINE.pid != 0
    End;
//
DELIMITER ;



DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
    BEGIN
        START TRANSACTION;
        CALL `UpdateCDLTables`();
        CALL `UpdateRatingEngineTable`();
        CALL `MigrateRatingModelTable`();
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
