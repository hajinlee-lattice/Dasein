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
        -- Tables for query infrastructure
        create table [METADATA_SEGMENT] ([PID] bigint identity not null unique, [CREATED] datetime2 not null, [NAME] nvarchar(255) not null, [RESTRICTION] varchar(MAX), [UPDATED] datetime2 not null, FK_QUERY_SOURCE_ID bigint not null, primary key ([PID]));
        create table [METADATA_DATA_COLLECTION] ([PID] bigint identity not null unique, [IS_DEFAULT] bit not null, [NAME] nvarchar(255) not null, [STATISTICS_ID] nvarchar(255), [TENANT_ID] bigint not null, FK_TENANT_ID bigint not null, primary key ([PID]));

        alter table [METADATA_SEGMENT] add constraint FK90C89E03382C8AE3 foreign key (FK_QUERY_SOURCE_ID) references [METADATA_DATA_COLLECTION] on delete cascade;
        create nonclustered index FK90C89E03382C8AE3 on [METADATA_SEGMENT] (FK_QUERY_SOURCE_ID)

        alter table [METADATA_DATA_COLLECTION] add constraint FKF69DFD4336865BC foreign key (FK_TENANT_ID) references [TENANT] on delete cascade;
        create nonclustered index FKF69DFD4336865BC on [METADATA_DATA_COLLECTION] (FK_TENANT_ID)


    COMMIT
GO


EXEC dbo.UpdateSchema;
GO


