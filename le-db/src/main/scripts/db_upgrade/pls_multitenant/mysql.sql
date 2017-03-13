USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
        -- Metadata Collection and Segment
        create table `METADATA_DATA_COLLECTION_PROPERTY` (`PID` bigint not null auto_increment unique, `PROPERTY` varchar(255) not null, `VALUE` varchar(2048), FK_DATA_COLLECTION_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
       create table `METADATA_DATA_COLLECTION` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, `TYPE` varchar(255) not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
        create table `METADATA_SEGMENT` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `NAME` varchar(255) not null, `RESTRICTION` longtext, `UPDATED` datetime not null, FK_QUERY_SOURCE_ID bigint not null, primary key (`PID`));
        create table `METADATA_SEGMENT_PROPERTY` (`PID` bigint not null auto_increment unique, `PROPERTY` varchar(255) not null, `VALUE` varchar(2048), METADATA_SEGMENT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;

        -- Metadata Collection and Segment indices and foreign keys
        create index DATA_COLLECTION_PROPERTY_IDX on `METADATA_DATA_COLLECTION_PROPERTY` (`PROPERTY`);
        alter table `METADATA_DATA_COLLECTION_PROPERTY` add index FKA6E03D51D089E196 (FK_DATA_COLLECTION_ID), add constraint FKA6E03D51D089E196 foreign key (FK_DATA_COLLECTION_ID) references `METADATA_DATA_COLLECTION` (`PID`) on delete cascade;
        alter table `METADATA_DATA_COLLECTION` add index FKF69DFD4336865BC (FK_TENANT_ID), add constraint FKF69DFD4336865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;
        alter table `METADATA_SEGMENT` add index FK90C89E03382C8AE3 (FK_QUERY_SOURCE_ID), add constraint FK90C89E03382C8AE3 foreign key (FK_QUERY_SOURCE_ID) references `METADATA_DATA_COLLECTION` (`PID`) on delete cascade;
        create index SEGMENT_PROPERTY_IDX on `METADATA_SEGMENT_PROPERTY` (`PROPERTY`);
        alter table `METADATA_SEGMENT_PROPERTY` add index FKF9BE749179DE23EE (METADATA_SEGMENT_ID), add constraint FKF9BE749179DE23EE foreign key (METADATA_SEGMENT_ID) references `METADATA_SEGMENT` (`PID`) on delete cascade;

        -- Metadata Table Relationship
        create table `METADATA_TABLE_RELATIONSHIP` (`PID` bigint not null auto_increment unique, `SOURCE_ATTRIBUTES` longtext, `SOURCE_CARDINALITY` integer not null, `TARGET_ATTRIBUTES` longtext, `TARGET_CARDINALITY` integer not null, `TARGET_TABLE_NAME` varchar(255) not null, FK_SOURCE_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
        alter table `METADATA_TABLE_RELATIONSHIP` add index FKE87FC63961584D17 (FK_SOURCE_TABLE_ID), add constraint FKE87FC63961584D17 foreign key (FK_SOURCE_TABLE_ID) references `METADATA_TABLE` (`PID`) on delete cascade;


        alter table `SOURCE_FILE` add `ENTITY_EXTERNAL_TYPE` varchar(255);

        create table `METADATA_HIERARCHY` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, FK_TABLE_ID bigint not null, primary key (`PID`));
        create table `METADATA_LEVEL` (`PID` bigint not null auto_increment unique, `ATTRIBUTES` varchar(2048) not null, `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, `ORDER_IN_HIERARCHY` integer not null, FK_HIERARCHY_ID bigint not null, primary key (`PID`));
        alter table `METADATA_HIERARCHY` add index FK603219055FC50F27 (FK_TABLE_ID), add constraint FK603219055FC50F27 foreign key (FK_TABLE_ID) references `METADATA_TABLE` (`PID`) on delete cascade;
        alter table `METADATA_LEVEL` add index FK41081254F80C45B8 (FK_HIERARCHY_ID), add constraint FK41081254F80C45B8 foreign key (FK_HIERARCHY_ID) references `METADATA_HIERARCHY` (`PID`) on delete cascade;

        --Metadata Vdb Extract
        create table `METADATA_VDB_EXTRACT` (`PID` bigint not null auto_increment unique, `EXTRACT_IDENTIFIER` varchar(255) not null unique, `EXTRACTION_TS` datetime not null, `LINES_PER_FILE` integer, `LOAD_APPLICATION_ID` varchar(255), `PROCESSED_RECORDS` integer not null, `IMPORT_STATUS` varchar(255) not null, `TARGET_PATH` varchar(2048), primary key (`PID`)) ENGINE=InnoDB;
        create index IX_EXTRACT_IDENTIFIER on `METADATA_VDB_EXTRACT` (`EXTRACT_IDENTIFIER`);
     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



