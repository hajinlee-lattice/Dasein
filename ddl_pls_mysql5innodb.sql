
    alter table `MODEL_SUMMARY` 
        drop 
        foreign key FKCEB8C45053984C5E;

    alter table `MODEL_SUMMARY` 
        drop 
        foreign key FKCEB8C45036865BC;

    alter table `PREDICTOR_ELEMENT` 
        drop 
        foreign key FKC39535F9F9802A4B;

    alter table `PREDICTOR` 
        drop 
        foreign key FK55BF005C8807F0C9;

    drop table if exists `KEY_VALUE`;

    drop table if exists `MODEL_SUMMARY`;

    drop table if exists `PREDICTOR_ELEMENT`;

    drop table if exists `PREDICTOR`;

    drop table if exists `TENANT`;

    create table `KEY_VALUE` (
        `PID` bigint not null auto_increment unique,
        `DATA` longblob not null,
        `OWNER_TYPE` varchar(255) not null,
        `TENANT_ID` bigint not null,
        primary key (`PID`)
    ) ENGINE=InnoDB;

    create table `MODEL_SUMMARY` (
        `PID` bigint not null auto_increment unique,
        `CONSTRUCTION_TIME` bigint not null,
        `DOWNLOADED` boolean not null,
        `ID` varchar(255) not null,
        `LOOKUP_ID` varchar(255) not null,
        `NAME` varchar(255) not null,
        `ROC_SCORE` double precision not null,
        `TENANT_ID` bigint not null,
        `TEST_CONVERSION_COUNT` bigint not null,
        `TEST_ROW_COUNT` bigint not null,
        `TOTAL_CONVERSION_COUNT` bigint not null,
        `TOTAL_ROW_COUNT` bigint not null,
        `TRAINING_CONVERSION_COUNT` bigint not null,
        `TRAINING_ROW_COUNT` bigint not null,
        FK_KEY_VALUE_ID bigint not null,
        FK_TENANT_ID bigint not null,
        primary key (`PID`)
    ) ENGINE=InnoDB;

    create table `PREDICTOR_ELEMENT` (
        `PID` bigint not null auto_increment unique,
        `CORRELATION_SIGN` integer not null,
        `COUNT` bigint not null,
        `LIFT` double precision not null,
        `LOWER_INCLUSIVE` double precision,
        `NAME` varchar(255) not null,
        `REVENUE` double precision not null,
        `TENANT_ID` bigint not null,
        `UNCERTAINTY_COEFF` double precision not null,
        `UPPER_EXCLUSIVE` double precision,
        `VALUES` longtext,
        `VISIBLE` boolean not null,
        FK_PREDICTOR_ID bigint not null,
        primary key (`PID`)
    ) ENGINE=InnoDB;

    create table `PREDICTOR` (
        `PID` bigint not null auto_increment unique,
        `APPROVED_USAGE` varchar(255),
        `CATEGORY` varchar(255) not null,
        `DISPLAY_NAME` varchar(255) not null,
        `FUNDAMENTAL_TYPE` varchar(255),
        `NAME` varchar(255) not null,
        `TENANT_ID` bigint not null,
        `UNCERTAINTY_COEFF` double precision,
        FK_MODELSUMMARY_ID bigint not null,
        primary key (`PID`)
    ) ENGINE=InnoDB;

    create table `TENANT` (
        `TENANT_PID` bigint not null auto_increment unique,
        `TENANT_ID` varchar(255) not null unique,
        `NAME` varchar(255) not null unique,
        `REGISTERED_TIME` bigint not null,
        primary key (`TENANT_PID`)
    ) ENGINE=InnoDB;

    create index KEY_VALUE_TENANT_ID_IDX on `KEY_VALUE` (`TENANT_ID`);

    create index MODEL_SUMMARY_ID_IDX on `MODEL_SUMMARY` (`ID`);

    alter table `MODEL_SUMMARY` 
        add index FKCEB8C45053984C5E (FK_KEY_VALUE_ID), 
        add constraint FKCEB8C45053984C5E 
        foreign key (FK_KEY_VALUE_ID) 
        references `KEY_VALUE` (`PID`) 
        on delete cascade;

    alter table `MODEL_SUMMARY` 
        add index FKCEB8C45036865BC (FK_TENANT_ID), 
        add constraint FKCEB8C45036865BC 
        foreign key (FK_TENANT_ID) 
        references `TENANT` (`TENANT_PID`) 
        on delete cascade;

    alter table `PREDICTOR_ELEMENT` 
        add index FKC39535F9F9802A4B (FK_PREDICTOR_ID), 
        add constraint FKC39535F9F9802A4B 
        foreign key (FK_PREDICTOR_ID) 
        references `PREDICTOR` (`PID`) 
        on delete cascade;

    alter table `PREDICTOR` 
        add index FK55BF005C8807F0C9 (FK_MODELSUMMARY_ID), 
        add constraint FK55BF005C8807F0C9 
        foreign key (FK_MODELSUMMARY_ID) 
        references `MODEL_SUMMARY` (`PID`) 
        on delete cascade;
