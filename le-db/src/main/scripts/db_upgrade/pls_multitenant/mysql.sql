USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
     START TRANSACTION;
	create table `LAUNCH` (`PID` bigint not null auto_increment unique, `STATE` integer not null, `NAME` varchar(255) not null, `TABLE_NAME` varchar(255), `TIMESTAMP` datetime not null, FK_PLAY_ID bigint, primary key (`PID`)) ENGINE=InnoDB;
	create table `PLAY` (`PID` bigint not null auto_increment unique, `DESCRIPTION` varchar(255), `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, `SEGMENT_NAME` varchar(255), `TENANT_ID` bigint not null, FK_CALL_PREP_ID bigint, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
	create table `CALL_PREP` (`PID` bigint not null auto_increment unique, primary key (`PID`)) ENGINE=InnoDB;

	alter table `LAUNCH` add index FK856C17B3E57F1489 (FK_PLAY_ID), add constraint FK856C17B3E57F1489 foreign key (FK_PLAY_ID) references `PLAY` (`PID`) on delete cascade;
	alter table `PLAY` add index FK258334883752BA (FK_CALL_PREP_ID), add constraint FK258334883752BA foreign key (FK_CALL_PREP_ID) references `CALL_PREP` (`PID`) on delete cascade;
	alter table `PLAY` add index FK25833436865BC (FK_TENANT_ID), add constraint FK25833436865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;

     COMMIT;
  END; //
DELIMITER ;

CALL `UpdateSchema`();



