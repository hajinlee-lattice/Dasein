
    alter table `DATAFLOW_JOB` 
        drop 
        foreign key FK4E6B9EB6E88B2327;

    drop table if exists `DATAFLOW_JOB`;

    drop table if exists `JOB`;

    create table `DATAFLOW_JOB` (
        `CUSTOMER` varchar(255),
        `DATAFLOW_BEAN_NAME` varchar(255),
        `JOB_PID` bigint not null,
        primary key (`JOB_PID`)
    ) ENGINE=InnoDB;

    create table `JOB` (
        `JOB_PID` bigint not null auto_increment unique,
        `APPMASTER_PROPERTIES` longtext,
        `CLIENT` varchar(255) not null,
        `CONTAINER_PROPERTIES` longtext,
        `CUSTOMER` varchar(255),
        `JOB_ID` varchar(255) not null,
        primary key (`JOB_PID`)
    ) ENGINE=InnoDB;

    alter table `DATAFLOW_JOB` 
        add index FK4E6B9EB6E88B2327 (`JOB_PID`), 
        add constraint FK4E6B9EB6E88B2327 
        foreign key (`JOB_PID`) 
        references `JOB` (`JOB_PID`);
