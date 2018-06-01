CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN

    ALTER TABLE `PLS_MultiTenant`.`AI_MODEL` add column `MODEL_SUMMARY_ID` varchar(255);
    ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` add column `JUST_CREATED` bit;

    UPDATE `PLS_MultiTenant`.`AI_MODEL` a
    INNER JOIN `PLS_MultiTenant`.`MODEL_SUMMARY` m ON m.`PID` = a.`FK_MODEL_SUMMARY_ID`
    SET a.`MODEL_SUMMARY_ID` = m.`ID`
    WHERE a.`FK_MODEL_SUMMARY_ID` IS NOT NULL;

    CREATE TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` (`PID` bigint not null auto_increment, `ACCOUNT_ID` varchar(255), `CREATED` datetime not null, `DESCRIPTION` varchar(255), `EXT_SYS_TYPE` varchar(255) not null, `ID` varchar(255) not null, `IS_REGISTERED` bit not null, `ORG_ID` varchar(255) not null, `ORG_NAME` varchar(255) not null, `UPDATED` datetime not null, `FK_TENANT_ID` bigint not null, primary key (`PID`)) engine=InnoDB;

    CREATE INDEX `LOOKUP_ID_MAP_CONFIG_ID` on `PLS_MultiTenant`.`LOOKUP_ID_MAP` (`ID`);
    ALTER TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` add constraint `UKfmshrnec538co7s37qhkrmk24` unique (`ORG_ID`, `EXT_SYS_TYPE`, `FK_TENANT_ID`);
    ALTER TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` add constraint `UK_sf0jp00syg0wvgudux06p4owu` unique (`ID`);
    ALTER TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` add constraint `FK_LOOKUPIDMAP_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;
    END;

    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` add column `DESTINATION_ACC_ID` varchar(255);
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` add column `DESTINATION_ORG_ID` varchar(255);
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` add column `DESTINATION_SYS_TYPE` varchar(255);

   create table `METADATA_DATA_COLLECTION_STATUS` (`PID` bigint not null auto_increment, `Detail` JSON, `VERSION` varchar(255) not null, `FK_COLLECTION_ID` bigint not null, `TENANT_ID` bigint not null, primary key (`PID`)) engine=InnoDB;
   alter table `METADATA_DATA_COLLECTION_STATUS` add constraint `FK_METADATADATACOLLECTIONSTATUS_FKCOLLECTIONID_METADATADATACOLLE` foreign key (`FK_COLLECTION_ID`) references `METADATA_DATA_COLLECTION` (`PID`) on delete cascade;
   alter table `METADATA_DATA_COLLECTION_STATUS` add constraint `FK_METADATADATACOLLECTIONSTATUS_TENANTID_TENANT` foreign key (`TENANT_ID`) references `TENANT` (`TENANT_PID`) on delete cascade;
//
DELIMITER ;

CREATE PROCEDURE `UpdateDataFeed_Execution`()
    BEGIN
        ALTER TABLE PLS_MultiTenant.DATAFEED_EXECUTION ADD WORKFLOW_PID BIGINT(20);

        UPDATE PLS_MultiTenant.DATAFEED_EXECUTION a
            INNER JOIN PLS_MultiTenant.WORKFLOW_JOB b ON a.WORKFLOW_ID = b.WORKFLOW_ID
        SET a.WORKFLOW_PID = b.PID;

        ALTER TABLE PLS_MultiTenant.DATAFEED_EXECUTION ADD COLUMN `UPDATED` DATETIME DEFAULT CURRENT_TIMESTAMP
                ON
                UPDATE CURRENT_TIMESTAMP;

        ALTER TABLE PLS_MultiTenant.DATAFEED_EXECUTION ADD COLUMN `CREATED` DATETIME DEFAULT CURRENT_TIMESTAMP;
    END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateActionTable`()
    BEGIN
        ALTER TABLE `PLS_MultiTenant`.`ACTION` add column `ACTION_CONFIGURATION` JSON;
    END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
    BEGIN
        START TRANSACTION;
        CALL `UpdateCDLTables`();
        CALL `UpdateDataFeed_Execution`();
        CALL `UpdateActionTable`()
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
