USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
        -- Tables for query infrastructure
        create table `METADATA_DATA_COLLECTION` (`PID` bigint not null auto_increment unique, `IS_DEFAULT` boolean not null, `NAME` varchar(255) not null, `STATISTICS_ID` varchar(255), `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`));
        create table `METADATA_SEGMENT` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `NAME` varchar(255) not null, `RESTRICTION` longtext, `UPDATED` datetime not null, FK_QUERY_SOURCE_ID bigint not null, primary key (`PID`));

        alter table `METADATA_DATA_COLLECTION` add index FKF69DFD4336865BC (FK_TENANT_ID), add constraint FKF69DFD4336865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;
        alter table `METADATA_SEGMENT` add index FK90C89E03382C8AE3 (FK_QUERY_SOURCE_ID), add constraint FK90C89E03382C8AE3 foreign key (FK_QUERY_SOURCE_ID) references `METADATA_DATA_COLLECTION` (`PID`) on delete cascade;
        alter table `SOURCE_FILE` add `ENTITY_EXTERNAL_TYPE` varchar(255);

        create table `METADATA_HIERARCHY` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, FK_TABLE_ID bigint not null, primary key (`PID`));
        create table `METADATA_LEVEL` (`PID` bigint not null auto_increment unique, `ATTRIBUTES` varchar(2048) not null, `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, `ORDER_IN_HIERARCHY` integer not null, FK_HIERARCHY_ID bigint not null, primary key (`PID`));
        alter table `METADATA_HIERARCHY` add index FK603219055FC50F27 (FK_TABLE_ID), add constraint FK603219055FC50F27 foreign key (FK_TABLE_ID) references `METADATA_TABLE` (`PID`) on delete cascade;
        alter table `METADATA_LEVEL` add index FK41081254F80C45B8 (FK_HIERARCHY_ID), add constraint FK41081254F80C45B8 foreign key (FK_HIERARCHY_ID) references `METADATA_HIERARCHY` (`PID`) on delete cascade;
     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



