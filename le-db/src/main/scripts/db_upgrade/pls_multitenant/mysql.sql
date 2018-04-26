CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN

    ALTER TABLE `PLS_MultiTenant`.`AI_MODEL` add column `MODEL_SUMMARY_ID` varchar(255);

    UPDATE `PLS_MultiTenant`.`AI_MODEL` a
    INNER JOIN `PLS_MultiTenant`.`MODEL_SUMMARY` m ON m.`PID` = a.`FK_MODEL_SUMMARY_ID`
    SET a.`MODEL_SUMMARY_ID` = m.`ID`
    WHERE a.`FK_MODEL_SUMMARY_ID` IS NOT NULL;

    CREATE TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` (`PID` bigint not null auto_increment, `ACCOUNT_ID` varchar(255), `CREATED` datetime not null, `DESCRIPTION` varchar(255), `EXT_SYS_TYPE` varchar(255) not null, `ID` varchar(255) not null, `ORG_ID` varchar(255) not null, `ORG_NAME` varchar(255) not null, `UPDATED` datetime not null, `FK_TENANT_ID` bigint not null, primary key (`PID`)) engine=InnoDB;
    CREATE INDEX `LOOKUP_ID_MAP_CONFIG_ID` on `PLS_MultiTenant`.`LOOKUP_ID_MAP` (`ID`);
    ALTER TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` add constraint `UKfmshrnec538co7s37qhkrmk24` unique (`ORG_ID`, `EXT_SYS_TYPE`, `FK_TENANT_ID`);
    ALTER TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` add constraint `UK_sf0jp00syg0wvgudux06p4owu` unique (`ID`);
    ALTER TABLE `PLS_MultiTenant`.`LOOKUP_ID_MAP` add constraint `FK_LOOKUPIDMAP_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;
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
