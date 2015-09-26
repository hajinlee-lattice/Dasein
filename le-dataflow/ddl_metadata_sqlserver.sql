
    alter table [DATAFLOW_JOB] 
        drop constraint FK4E6B9EB6E88B2327;

    drop table [DATAFLOW_JOB];

    drop table [JOB];

    create table [DATAFLOW_JOB] (
        [CUSTOMER] nvarchar(255),
        [DATAFLOW_BEAN_NAME] nvarchar(255),
        [JOB_PID] bigint not null,
        primary key ([JOB_PID])
    );

    create table [JOB] (
        [JOB_PID] bigint identity not null unique,
        [APPMASTER_PROPERTIES] nvarchar(MAX),
        [CLIENT] nvarchar(255) not null,
        [CONTAINER_PROPERTIES] nvarchar(MAX),
        [CUSTOMER] nvarchar(255),
        [JOB_ID] nvarchar(255) not null,
        primary key ([JOB_PID])
    );

    alter table [DATAFLOW_JOB] 
        add constraint FK4E6B9EB6E88B2327 
        foreign key ([JOB_PID]) 
        references [JOB];
