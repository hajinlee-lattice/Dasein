USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
        -- Tables for query infrastructure
        create table `METADATA_DATA_COLLECTION` (`PID` bigint not null auto_increment unique, `IS_DEFAULT` boolean not null, `NAME` varchar(255) not null, `STATISTICS_ID` varchar(255), `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`));
        create table `METADATA_SEGMENT` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `NAME` varchar(255) not null, `RESTRICTION` longtext, `UPDATED` datetime not null, FK_QUERY_SOURCE_ID bigint not null, primary key (`PID`));

        alter table `METADATA_DATA_COLLECTION` add index FKF69DFD4336865BC (FK_TENANT_ID), add constraint FKF69DFD4336865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;
        alter table `METADATA_SEGMENT` add index FK90C89E03382C8AE3 (FK_QUERY_SOURCE_ID), add constraint FK90C89E03382C8AE3 foreign key (FK_QUERY_SOURCE_ID) references `METADATA_DATA_COLLECTION` (`PID`) on delete cascade;
     COMMIT;
  END;

CALL `UpdateSchema`();



