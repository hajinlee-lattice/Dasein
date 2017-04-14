USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
        create table `METADATA_DEPENDABLE` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, `TYPE` varchar(255) not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`, `TYPE`)) ENGINE=InnoDB;
        create table `METADATA_DEPENDENCY_LINK` (`PID` bigint not null auto_increment unique, `CHILD_NAME` varchar(255) not null, `CHILD_TYPE` varchar(255) not null, FK_PARENT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;

        alter table `METADATA_DEPENDABLE` add index FK603E28D636865BC (FK_TENANT_ID), add constraint FK603E28D636865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;
        alter table `METADATA_DEPENDENCY_LINK` add index FK408EFB5E54072A8 (FK_PARENT_ID), add constraint FK408EFB5E54072A8 foreign key (FK_PARENT_ID) references `METADATA_DEPENDABLE` (`PID`) on delete cascade;
        create index NAME_TYPE_IDX on `METADATA_DEPENDENCY_LINK` (`CHILD_NAME`, `CHILD_TYPE`);

     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



