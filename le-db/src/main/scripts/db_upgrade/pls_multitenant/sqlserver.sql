USE [PLS_MultiTenant]

IF EXISTS ( SELECT *
            FROM   sysobjects
            WHERE  id = object_id(N'[dbo].[UpdateSchema]')
                   AND OBJECTPROPERTY(id, N'IsProcedure') = 1 )
BEGIN
    DROP PROCEDURE [dbo].[UpdateSchema]
END
GO

CREATE PROCEDURE UpdateSchema
AS
    BEGIN TRANSACTION
        -- Metadata Collection and Segment
        create table [METADATA_DATA_COLLECTION_PROPERTY] ([PID] bigint identity not null unique, [PROPERTY] nvarchar(255) not null, [VALUE] nvarchar(2048), FK_DATA_COLLECTION_ID bigint not null, primary key ([PID]));
        create table [METADATA_DATA_COLLECTION] ([PID] bigint identity not null unique, [NAME] nvarchar(255) not null, [TENANT_ID] bigint not null, [TYPE] nvarchar(255) not null, FK_TENANT_ID bigint not null, primary key ([PID]));
        create table [METADATA_SEGMENT_PROPERTY] ([PID] bigint identity not null unique, [PROPERTY] nvarchar(255) not null, [VALUE] nvarchar(2048), METADATA_SEGMENT_ID bigint not null, primary key ([PID]));
        create table [METADATA_SEGMENT] ([PID] bigint identity not null unique, [CREATED] datetime2 not null, [DESCRIPTION] nvarchar(255), [DISPLAY_NAME] nvarchar(255) not null, [NAME] nvarchar(255) not null, [RESTRICTION] varchar(MAX), [UPDATED] datetime2 not null, FK_QUERY_SOURCE_ID bigint not null, primary key ([PID]));

        -- Metadata Collection and Segment indices and foreign keys
        create index SEGMENT_PROPERTY_IDX on [METADATA_SEGMENT_PROPERTY] ([PROPERTY]);
alter table [METADATA_SEGMENT_PROPERTY] add constraint FKF9BE749179DE23EE foreign key (METADATA_SEGMENT_ID) references [METADATA_SEGMENT] on delete cascade;
        create nonclustered index FKF9BE749179DE23EE on [METADATA_SEGMENT_PROPERTY] (METADATA_SEGMENT_ID)
        alter table [METADATA_SEGMENT] add constraint FK90C89E03382C8AE3 foreign key (FK_QUERY_SOURCE_ID) references [METADATA_DATA_COLLECTION] on delete cascade;
        create nonclustered index FK90C89E03382C8AE3 on [METADATA_SEGMENT] (FK_QUERY_SOURCE_ID)
        create index DATA_COLLECTION_PROPERTY_IDX on [METADATA_DATA_COLLECTION_PROPERTY] ([PROPERTY]);
        alter table [METADATA_DATA_COLLECTION_PROPERTY] add constraint FKA6E03D51D089E196 foreign key (FK_DATA_COLLECTION_ID) references [METADATA_DATA_COLLECTION] on delete cascade;
        create nonclustered index FKA6E03D51D089E196 on [METADATA_DATA_COLLECTION_PROPERTY] (FK_DATA_COLLECTION_ID)
        alter table [METADATA_DATA_COLLECTION] add constraint FKF69DFD4336865BC foreign key (FK_TENANT_ID) references [TENANT] on delete cascade;
        create nonclustered index FKF69DFD4336865BC on [METADATA_DATA_COLLECTION] (FK_TENANT_ID)

        -- Metadata Table Relationship
        create table [METADATA_TABLE_RELATIONSHIP] ([PID] bigint identity not null unique, [SOURCE_ATTRIBUTES] varchar(MAX), [SOURCE_CARDINALITY] int not null, [TARGET_ATTRIBUTES] varchar(MAX), [TARGET_CARDINALITY] int not null, [TARGET_TABLE_NAME] nvarchar(255) not null, FK_SOURCE_TABLE_ID bigint not null, primary key ([PID]));
        alter table [METADATA_TABLE_RELATIONSHIP] add constraint FKE87FC63961584D17 foreign key (FK_SOURCE_TABLE_ID) references [METADATA_TABLE] on delete cascade;
        create nonclustered index FKE87FC63961584D17 on [METADATA_TABLE_RELATIONSHIP] (FK_SOURCE_TABLE_ID)

        -- Metadata vdb extract
        create table [METADATA_VDB_EXTRACT] ([PID] bigint identity not null unique, [EXTRACT_IDENTIFIER] nvarchar(255) not null unique, [EXTRACTION_TS] datetime2 not null, [LINES_PER_FILE] int, [LOAD_APPLICATION_ID] nvarchar(255), [PROCESSED_RECORDS] int not null, [IMPORT_STATUS] nvarchar(255) not null, [TARGET_PATH] nvarchar(2048), primary key ([PID]));
        create index IX_EXTRACT_IDENTIFIER on [METADATA_VDB_EXTRACT] ([EXTRACT_IDENTIFIER]);

    COMMIT
GO


EXEC dbo.UpdateSchema;
GO


