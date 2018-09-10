CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `CONTRACT` varchar(255) DEFAULT NULL;
    update `PLS_MultiTenant`.`TENANT` set STATUS='ACTIVE' where STATUS is NULL;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `STATUS` varchar(255) NOT NULL;

    update `PLS_MultiTenant`.`TENANT` set TENANT_TYPE = 'CUSTOMER' where TENANT_TYPE is NULL;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `TENANT_TYPE` varchar(255) NOT NULL;

    ALTER TABLE `PLS_MultiTenant`.`PLAY` MODIFY COLUMN `DESCRIPTION` varchar(255) default NULL;

    create table `PLS_MultiTenant`.`PLAY_TYPE` (
    `PID` bigint not null auto_increment,
    `CREATED` datetime not null,
    `CREATED_BY` varchar(255) not null,
    `DESCRIPTION` varchar(8192),
    `DISPLAY_NAME` varchar(255) not null,
    `ID` varchar(255) not null,
    `TENANT_ID` bigint not null,
    `UPDATED` datetime not null,
    `UPDATED_BY` varchar(255) not null,
    `FK_TENANT_ID` bigint not null,
    primary key (`PID`))
    engine=InnoDB;

    alter table `PLS_MultiTenant`.`PLAY_TYPE` add constraint `FK_PLAYTYPE_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;

    alter table `PLS_MultiTenant`.`PLAY` add column `FK_PLAY_TYPE` bigint;
    alter table `PLS_MultiTenant`.`PLAY` add constraint `FK_PLAY_FKPLAYTYPE_PLAYTYPE` foreign key (`FK_PLAY_TYPE`) references `PLS_MultiTenant`.`PLAY_TYPE` (`PID`);
  END;
//
DELIMITER ;

CREATE PROCEDURE `CreateDropBoxTable`()
  BEGIN
    create table `ATLAS_DROPBOX` (
      `PID`          bigint not null auto_increment,
      `DROPBOX`      varchar(8),
      `FK_TENANT_ID` bigint not null,
      primary key (`PID`)
    )
      engine = InnoDB;
    alter table `ATLAS_DROPBOX`
      add constraint `UX_DROPBOX` unique (`DROPBOX`);
    alter table `ATLAS_DROPBOX`
      add constraint `FK_ATLASDROPBOX_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`)
      on delete cascade;
  END;
//
DELIMITER ;

CALL `UpdatePLSTables`();
CALL `CreateDropBoxTable`();

