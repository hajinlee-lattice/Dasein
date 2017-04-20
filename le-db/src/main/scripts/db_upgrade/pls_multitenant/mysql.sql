USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
        create table `METADATA_DEPENDABLE` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, `TYPE` integer not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`, `TYPE`)) ENGINE=InnoDB;
        create table `METADATA_DEPENDENCY_LINK` (`PID` bigint not null auto_increment unique, `CHILD_NAME` varchar(255) not null, `CHILD_TYPE` integer not null, FK_PARENT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;

        alter table `METADATA_DEPENDABLE` add index FK603E28D636865BC (FK_TENANT_ID), add constraint FK603E28D636865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;
        alter table `METADATA_DEPENDENCY_LINK` add index FK408EFB5E54072A8 (FK_PARENT_ID), add constraint FK408EFB5E54072A8 foreign key (FK_PARENT_ID) references `METADATA_DEPENDABLE` (`PID`) on delete cascade;
        create index NAME_TYPE_IDX on `METADATA_DEPENDENCY_LINK` (`CHILD_NAME`, `CHILD_TYPE`);

	create table `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY` (FK_ATTRIBUTE_ID bigint not null, FK_SEGMENT_ID bigint not null) ENGINE=InnoDB;
	alter table `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY` add index FKD5E5A1AAFCF94C70 (FK_SEGMENT_ID), add constraint FKD5E5A1AAFCF94C70 foreign key (FK_SEGMENT_ID) references `METADATA_ATTRIBUTE` (`PID`);
        alter table `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY` add index FKD5E5A1AAA326A20F (FK_ATTRIBUTE_ID), add constraint FKD5E5A1AAA326A20F foreign key (FK_ATTRIBUTE_ID) references `METADATA_SEGMENT` (`PID`);

	create table `DATAFLOW_JOB_SOURCE_TABLE` (FK_JOB_ID bigint not null, `TABLE_NAME` varchar(255)) ENGINE=InnoDB;
	alter table `DATAFLOW_JOB` add column `TARGET_TABLE_NAME` varchar(255)
	alter table `DATAFLOW_JOB_SOURCE_TABLE` add index FKD48325738DEB5918 (FK_JOB_ID), add constraint FKD48325738DEB5918 foreign key (FK_JOB_ID) references `DATAFLOW_JOB` (`JOB_PID`);

     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



