CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN
        ALTER TABLE PLS_MultiTenant.PLAY
        ADD COLUMN `STATUS` varchar(255) null;
    
        ALTER TABLE PLS_MultiTenant.AI_MODEL
        ADD COLUMN `MODELING_STRATEGY` varchar(255) not null DEFAULT 'CROSS_SELL_REPEAT_PURCHASE';

        ALTER TABLE PLS_MultiTenant.AI_MODEL
        ADD COLUMN `PREDICTION_TYPE` varchar(255) not null DEFAULT 'PROPENSITY';

        UPDATE `PLS_MultiTenant`.`AI_MODEL`
            SET PREDICTION_TYPE = CASE WHEN MODELING_METHOD IS NULL THEN 'PROPENSITY' ELSE MODELING_METHOD END;

        ALTER TABLE `PLS_MultiTenant`.`METADATA_STATISTICS`
            ADD COLUMN CUBES_DATA longblob NULL,
            CHANGE COLUMN DATA DATA longblob NULL;

        ALTER TABLE `PLS_MultiTenant`.`METADATA_DATA_COLLECTION`
            ADD COLUMN `DATA_CLOUD_VERSION` VARCHAR(255);

        --ALTER TABLE PLS_MultiTenant.AI_MODEL DROP COLUMN `MODELING_METHOD`; -- Drop after both stacks are off of the codebase using this column
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateSelectedAttribute`()
    BEGIN
        alter table `PLS_MultiTenant`.`SELECTED_ATTRIBUTE` add column `DATA_LICENSE` varchar(255);
        update `PLS_MultiTenant`.`SELECTED_ATTRIBUTE` SET DATA_LICENSE = 'HG' WHERE IS_PREMIUM = 1 AND DATA_LICENSE is NULL;
    END
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
        CALL `UpdateSelectedAttribute`();
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
