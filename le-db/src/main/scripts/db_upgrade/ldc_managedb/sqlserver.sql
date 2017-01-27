USE [LDC_ManageDB]
GO

IF NOT EXISTS(
    SELECT *
    FROM sys.columns
    WHERE Name      = N'DynamoTableSignature_Lookup'
          AND Object_ID = Object_ID(N'DataCloudVersion'))
  BEGIN
    ALTER TABLE [DataCloudVersion] ADD [DynamoTableSignature_Lookup] NVARCHAR(100);
  END
GO
