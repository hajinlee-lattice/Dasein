USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
	create table `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY` (FK_ATTRIBUTE_ID bigint not null, FK_SEGMENT_ID bigint not null) ENGINE=InnoDB;
	alter table `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY` add index FKD5E5A1AAFCF94C70 (FK_SEGMENT_ID), add constraint FKD5E5A1AAFCF94C70 foreign key (FK_SEGMENT_ID) references `METADATA_ATTRIBUTE` (`PID`);
        alter table `METADATA_SEGMENT_ATTRIBUTE_DEPENDENCY` add index FKD5E5A1AAA326A20F (FK_ATTRIBUTE_ID), add constraint FKD5E5A1AAA326A20F foreign key (FK_ATTRIBUTE_ID) references `METADATA_SEGMENT` (`PID`);

	create table `DATAFLOW_JOB_SOURCE_TABLE` (FK_JOB_ID bigint not null, `TABLE_NAME` varchar(255)) ENGINE=InnoDB;
	alter table `DATAFLOW_JOB` add column `TARGET_TABLE_NAME` varchar(255)
	alter table `DATAFLOW_JOB_SOURCE_TABLE` add index FKD48325738DEB5918 (FK_JOB_ID), add constraint FKD48325738DEB5918 foreign key (FK_JOB_ID) references `DATAFLOW_JOB` (`JOB_PID`);

        alter table METADATA_DATA_COLLECTION add unique IDX_UNIQUE (`TENANT_ID`, `TYPE`);
        alter table 'Execution_Log' add 'RetryCount' int default 0 not null;
     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



