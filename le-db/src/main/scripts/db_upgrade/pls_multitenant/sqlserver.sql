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

    COMMIT
GO


EXEC dbo.UpdateSchema;
GO


