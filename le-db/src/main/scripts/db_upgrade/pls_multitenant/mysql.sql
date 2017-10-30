USE `PLS_MultiTenant`;

create table `CDL_JOB_DETAIL` (`PID` bigint not null auto_increment unique, `APPLICATION_ID` varchar(255), `CDL_JOB_STATUS` varchar(255) not null, `CDL_JOB_TYPE` varchar(255) not null, `CREATE_DATE` datetime, `LAST_UPDATE_DATE` datetime, `RETRY_COUNT` integer not null, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `CDL_JOB_DETAIL` add index FK41487BE736865BC (FK_TENANT_ID), add constraint FK41487BE736865BC foreign key (FK_TENANT_ID) references `TENANT` (`TENANT_PID`) on delete cascade;

create table `RATING_ENGINE_NOTE` (`PID` bigint not null auto_increment, `CREATED_BY_USER` varchar(255), `CREATION_TIMESTAMP` bigint not null, `ID` varchar(255) not null unique, `LAST_MODIFICATION_TIMESTAMP` bigint not null, `LAST_MODIFIED_BY_USER` varchar(255), `NOTES_CONTENTS` varchar(2048), `ORIGIN` varchar(255), FK_RATING_ENGINE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `RATING_ENGINE_NOTE` add index FKCF998C2D94623258 (FK_RATING_ENGINE_ID), add constraint FKCF998C2D94623258 foreign key (FK_RATING_ENGINE_ID) references `RATING_ENGINE` (`PID`) on delete cascade;

create table `MODEL_NOTE` (`PID` bigint not null auto_increment, `CREATED_BY_USER` varchar(255), `CREATION_TIMESTAMP` bigint not null, `ID` varchar(255) not null unique, `LAST_MODIFICATION_TIMESTAMP` bigint not null, `LAST_MODIFIED_BY_USER` varchar(255), `NOTES_CONTENTS` varchar(2048), `ORIGIN` varchar(255), `PARENT_MODEL_ID` varchar(255), MODEL_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
alter table `MODEL_NOTE` add index FK9C22DB68815935F7 (MODEL_ID), add constraint FK9C22DB68815935F7 foreign key (MODEL_ID) references `MODEL_SUMMARY` (`PID`) on delete cascade;

insert into `MODEL_NOTE` (`CREATED_BY_USER`, `CREATION_TIMESTAMP`, `ID`, `LAST_MODIFICATION_TIMESTAMP`, `LAST_MODIFIED_BY_USER`, `NOTES_CONTENTS`, `ORIGIN`, `MODEL_ID`, `PARENT_MODEL_ID`)
select `CREATED_BY_USER`, `CREATION_TIMESTAMP`, `ID`, `LAST_MODIFICATION_TIMESTAMP`, `LAST_MODIFIED_BY_USER`, `NOTES_CONTENTS`, `ORIGIN`, `MODEL_ID`, `PARENT_MODEL_ID`
from `MODEL_NOTES`;

ALTER TABLE `PLS_MultiTenant`.`DATAFEED`
    ADD COLUMN `AUTO_SCHEDULING` boolean not null DEFAULT 0, 
    ADD COLUMN `DRAINING_STATUS` varchar(255) not null DEFAULT 'NONE';

