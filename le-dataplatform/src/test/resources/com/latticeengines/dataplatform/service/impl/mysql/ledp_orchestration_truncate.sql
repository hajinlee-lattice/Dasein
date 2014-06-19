use [LeadScoringDB]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

update LeadScoringDB.dbo.LeadScoringCommand set CommandStatus=0, ModelCommandStep=NULL where CommandId=186
truncate table [LeadScoringDB].[dbo].[LeadScoringCommandLog]
GO
truncate table [LeadScoringDB].[dbo].[LeadScoringCommandState]
GO
truncate table [LeadScoringDB].[dbo].[LeadScoringResult]
GO


insert into DataForScoring_Lattice_Bernard select * from DataForScoring_Lattice


INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'MetadataTable'
           ,'EventMetadata_Nutanix');
INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'EventTable'
           ,'Q_EventTable_Nutanix');


INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'DepivotedEventTable'
           ,'Q_EventTableDepivot_Nutanix');
INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'KeyCols'
           ,'Nutanix_EventTable_Clean');
INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'ModelName'
           ,'Model Submission1');
INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'ModelTargets'
           ,'P1_Event');
INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'ExcludeColumns'
           ,'PeriodID,CustomerID,P1_Event,P1_TargetTraining,P1_Target,AwardYear,FundingFiscalQuarter,FundingFiscalYear,BusinessAssets,BusinessEntityType,BusinessIndustrySector,RetirementAssetsEOY,RetirementAssetsYOY,TotalParticipantsSOY,BusinessType,LeadID,Company, Domain,Email,LeadSource');
INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
     VALUES (180, 'NumSamples'
           ,'1');                                                        
GO

           ([CommandId]
           ,[Key]
           ,[Value])


INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommand]
           ([CommandId]
           ,[ModelId]
           ,[LeadInputTableName]
           ,[AnalysisServer]
           ,[CreateBy]
           ,[CreateTime]
           ,[CommandStatus]
           ,[Contract_External_ID]
           ,[Deployment_External_ID])
            select 
           180
           ,[ModelId]
           ,[LeadInputTableName]
           ,[AnalysisServer]
           ,[CreateBy]
           ,[CreateTime]
           ,0
           ,[Contract_External_ID]
           ,[Deployment_External_ID]
             from [LeadScoringDB].[dbo].[LeadScoringCommand];
GO 

INSERT INTO [LeadScoringDB].[dbo].[LeadScoringCommand]
           (
           [ModelId]
           ,[LeadInputTableName]
           ,[AnalysisServer]
           ,[CreateBy]
           ,[CreateTime]
           ,[CommandStatus]
           ,[Contract_External_ID]
           ,[Deployment_External_ID])
     VALUES
           (
           "BernardTestModelId"
           ,"BernardTestLeadInputTableName"
           ,"BernardTestAnalysisServer"
           ,"BernardTest"
           ,2014-06-17
           ,0
           ,"BernardTestContract_External_ID"
           ,"Nutanix")
GO


update LeadScoringDB.dbo.LeadScoringCommand set CommandStatus=4 where CommandId=179
update LeadScoringDB.dbo.LeadScoringCommand set Deployment_External_ID='Nutanix' where CommandId=180


update LeadScoringDB.dbo.LeadScoringCommand set CommandStatus=0 where CommandId=180
truncate table [LeadScoringDB].[dbo].[LeadScoringCommandLog]
GO
truncate table [LeadScoringDB].[dbo].[LeadScoringCommandState]
GO



truncate table [LeadScoringDB].[dbo].[LeadScoringCommandParameter]
GO

truncate table [LeadScoringDB].[dbo].[LeadScoringCommand]
GO