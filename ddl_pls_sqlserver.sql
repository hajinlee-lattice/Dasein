
    alter table [MODEL_SUMMARY] 
        drop constraint FKCEB8C45053984C5E;

    alter table [MODEL_SUMMARY] 
        drop constraint FKCEB8C45036865BC;

    alter table [PREDICTOR_ELEMENT] 
        drop constraint FKC39535F9F9802A4B;

    alter table [PREDICTOR] 
        drop constraint FK55BF005C8807F0C9;

    drop table [KEY_VALUE];

    drop table [MODEL_SUMMARY];

    drop table [PREDICTOR_ELEMENT];

    drop table [PREDICTOR];

    drop table [TENANT];

    create table [KEY_VALUE] (
        [PID] bigint identity not null unique,
        [DATA] varbinary(MAX) not null,
        [OWNER_TYPE] nvarchar(255) not null,
        [TENANT_ID] bigint not null,
        primary key ([PID])
    );

    create table [MODEL_SUMMARY] (
        [PID] bigint identity not null unique,
        [CONSTRUCTION_TIME] bigint not null,
        [DOWNLOADED] bit not null,
        [ID] nvarchar(255) not null,
        [LOOKUP_ID] nvarchar(255) not null,
        [NAME] nvarchar(255) not null,
        [ROC_SCORE] double precision not null,
        [TENANT_ID] bigint not null,
        [TEST_CONVERSION_COUNT] bigint not null,
        [TEST_ROW_COUNT] bigint not null,
        [TOTAL_CONVERSION_COUNT] bigint not null,
        [TOTAL_ROW_COUNT] bigint not null,
        [TRAINING_CONVERSION_COUNT] bigint not null,
        [TRAINING_ROW_COUNT] bigint not null,
        FK_KEY_VALUE_ID bigint not null,
        FK_TENANT_ID bigint not null,
        primary key ([PID])
    );

    create table [PREDICTOR_ELEMENT] (
        [PID] bigint identity not null unique,
        [CORRELATION_SIGN] int not null,
        [COUNT] bigint not null,
        [LIFT] double precision not null,
        [LOWER_INCLUSIVE] double precision,
        [NAME] nvarchar(255) not null,
        [REVENUE] double precision not null,
        [TENANT_ID] bigint not null,
        [UNCERTAINTY_COEFF] double precision not null,
        [UPPER_EXCLUSIVE] double precision,
        [VALUES] nvarchar(MAX),
        [VISIBLE] bit not null,
        FK_PREDICTOR_ID bigint not null,
        primary key ([PID])
    );

    create table [PREDICTOR] (
        [PID] bigint identity not null unique,
        [APPROVED_USAGE] nvarchar(255),
        [CATEGORY] nvarchar(255) not null,
        [DISPLAY_NAME] nvarchar(255) not null,
        [FUNDAMENTAL_TYPE] nvarchar(255),
        [NAME] nvarchar(255) not null,
        [TENANT_ID] bigint not null,
        [UNCERTAINTY_COEFF] double precision,
        FK_MODELSUMMARY_ID bigint not null,
        primary key ([PID])
    );

    create table [TENANT] (
        [TENANT_PID] bigint identity not null unique,
        [TENANT_ID] nvarchar(255) not null unique,
        [NAME] nvarchar(255) not null unique,
        [REGISTERED_TIME] bigint not null,
        primary key ([TENANT_PID])
    );

    create index KEY_VALUE_TENANT_ID_IDX on [KEY_VALUE] ([TENANT_ID]);

    create index MODEL_SUMMARY_ID_IDX on [MODEL_SUMMARY] ([ID]);

    alter table [MODEL_SUMMARY] 
        add constraint FKCEB8C45053984C5E 
        foreign key (FK_KEY_VALUE_ID) 
        references [KEY_VALUE] 
        on delete cascade;

    alter table [MODEL_SUMMARY] 
        add constraint FKCEB8C45036865BC 
        foreign key (FK_TENANT_ID) 
        references [TENANT] 
        on delete cascade;

    alter table [PREDICTOR_ELEMENT] 
        add constraint FKC39535F9F9802A4B 
        foreign key (FK_PREDICTOR_ID) 
        references [PREDICTOR] 
        on delete cascade;

    alter table [PREDICTOR] 
        add constraint FK55BF005C8807F0C9 
        foreign key (FK_MODELSUMMARY_ID) 
        references [MODEL_SUMMARY] 
        on delete cascade;
