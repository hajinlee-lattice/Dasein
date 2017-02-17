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

IF NOT EXISTS(
    SELECT *
    FROM sys.columns
    WHERE Name      = N'GroupTotal'
          AND Object_ID = Object_ID(N'AccountMasterFact'))
  BEGIN
    ALTER TABLE [AccountMasterFact] ADD [GroupTotal] BIGINT;
    ALTER TABLE [AccountMasterFact] ADD [AttrCount1] NVARCHAR(MAX);
    ALTER TABLE [AccountMasterFact] ADD [AttrCount2] NVARCHAR(MAX);
    ALTER TABLE [AccountMasterFact] ADD [AttrCount3] NVARCHAR(MAX);
    ALTER TABLE [AccountMasterFact] ADD [AttrCount4] NVARCHAR(MAX);
  END
GO
