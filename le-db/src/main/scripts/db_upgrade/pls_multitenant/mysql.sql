CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `TENANT_TYPE` varchar(255) DEFAULT NULL;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `STATUS` varchar(255) DEFAULT NULL;

    ALTER TABLE `PLS_MultiTenant`.`RATING_MODEL` ADD `CREATED_BY` varchar(255);

    ALTER TABLE PLS_MultiTenant.DATAFEED ADD COLUMN`NEXT_INVOKE_TIME` datetime;
  END;
//
DELIMITER ;

CREATE PROCEDURE `MarketoIntegrationSchema`()
  BEGIN
    create table `PLS_MultiTenant`.`SCORING_REQUEST_CONFIG` (`PID` bigint not null auto_increment, `REQ_CONFIG_ID` varchar(255) not null, `CREATED` datetime not null, `MODEL_UUID` varchar(255) not null, `UPDATED` datetime not null, `MARKETO_CREDENTIAL_ID` bigint not null, `FK_TENANT_ID` bigint not null, primary key (`PID`)) engine=InnoDB;
    alter table `PLS_MultiTenant`.`SCORING_REQUEST_CONFIG` add constraint SCORING_REQUEST_CONFIG.REQ_CONFIG_ID unique (`REQ_CONFIG_ID`);
    alter table `PLS_MultiTenant`.`SCORING_REQUEST_CONFIG` add constraint `FK_SCORINGREQUESTCONFIG_MARKETOCREDENTIALID_MARKETOCREDENTIAL` foreign key (`MARKETO_CREDENTIAL_ID`) references `PLS_MultiTenant`.`MARKETO_CREDENTIAL` (`PID`) on delete cascade;
    alter table `PLS_MultiTenant`.`SCORING_REQUEST_CONFIG` add constraint `FK_SCORINGREQUESTCONFIG_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;

    create table `PLS_MultiTenant`.`MARKETO_SCORING_MATCH_FIELD` (`PID` bigint not null auto_increment, `MARKETO_FIELD_NAME` varchar(255), `MODEL_FIELD_NAME` varchar(255), `SCORING_REQUEST_ID` bigint not null, `FK_TENANT_ID` bigint not null, primary key (`PID`)) engine=InnoDB;
    alter table `PLS_MultiTenant`.`MARKETO_SCORING_MATCH_FIELD` add constraint `FK_MARKETOSCORINGMATCHFIELD_SCORINGREQUESTID_SCORINGREQUESTCONFI` foreign key (`SCORING_REQUEST_ID`) references `PLS_MultiTenant`.`SCORING_REQUEST_CONFIG` (`PID`) on delete cascade;
    alter table `PLS_MultiTenant`.`MARKETO_SCORING_MATCH_FIELD` add constraint `FK_MARKETOSCORINGMATCHFIELD_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;

  END;
//
DELIMITER ;

CALL `UpdatePLSTables`();
CALL `MarketoIntegrationSchema`();
