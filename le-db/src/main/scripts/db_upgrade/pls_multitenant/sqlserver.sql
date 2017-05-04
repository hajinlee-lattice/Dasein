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

        -- Eai Import Job detail table
        create table [EAI_IMPORT_JOB_DETAIL] ([PID] bigint identity not null unique, [COLLECTION_IDENTIFIER] nvarchar(255) not null unique, [COLLECTION_TS] datetime2 not null, [LOAD_APPLICATION_ID] nvarchar(255), [PROCESSED_RECORDS] int not null, [SOURCE_TYPE] nvarchar(255) not null, [IMPORT_STATUS] nvarchar(255) not null, [TARGET_PATH] nvarchar(2048), primary key ([PID]));

    COMMIT
GO


EXEC dbo.UpdateSchema;
GO


