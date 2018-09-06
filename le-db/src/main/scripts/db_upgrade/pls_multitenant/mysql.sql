CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN

    ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `CONTRACT` varchar(255) DEFAULT NULL;
    update `PLS_MultiTenant`.`TENANT` set STATUS='ACTIVE' where STATUS is NULL
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `STATUS` varchar(255) NOT NULL;

    update `PLS_MultiTenant`.`TENANT` set TENANT_TYPE = 'CUSTOMER' where TENANT_TYPE is NULL
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `TENANT_TYPE` varchar(255) NOT NULL
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

