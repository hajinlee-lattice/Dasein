use [dataplatformtest]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('LeadScoringCommandLog', 'U') IS NOT NULL
    drop table [LeadScoringCommandLog];
GO
IF OBJECT_ID('LeadScoringCommandParameter', 'U') IS NOT NULL
    drop table [LeadScoringCommandParameter];
GO
IF OBJECT_ID('LeadScoringCommandState', 'U') IS NOT NULL
    drop table [LeadScoringCommandState];
GO
IF OBJECT_ID('LeadScoringCommand', 'U') IS NOT NULL
    drop table [LeadScoringCommand];
GO
 

    create table [LeadScoringCommandLog] (
        [PID] int identity not null unique,
        [Created] datetime,
        [Message] [nvarchar](max) not null,
        CommandId int not null,
        primary key ([PID])
    );
GO
    create table [LeadScoringCommandParameter] (
        [Key] varchar(255) not null,
        [Value] [nvarchar](max) not null,
        CommandId int not null,
        primary key ([Key], CommandId)
    );
GO
    create table [LeadScoringCommandState] (
        [PID] int identity not null unique,
        [Created] datetime,        
        [Diagnostics] [nvarchar](max) not null,
        [ElapsedTimeInMillis] int,
        [ModelCommandStep] varchar(255) not null,
        [Progress] float,
        [Status] varchar(255),
        [TrackingUrl] [nvarchar](max) not null,
        [YarnApplicationId] varchar(255),
        CommandId int not null,
        primary key ([PID])
    );
GO
    create table [LeadScoringCommand] (
        [CommandId] int not null unique,
        [CommandStatus] int not null,
        [Deployment_External_ID] varchar(255) not null,
        [ModelCommandStep] varchar(255),
        primary key ([CommandId])
    );
GO

    alter table [LeadScoringCommandLog] 
        add constraint FK73D5FC0CB951A41C 
        foreign key (CommandId) 
        references [LeadScoringCommand];
GO
    alter table [LeadScoringCommandParameter] 
        add constraint FK318F5671B951A41C 
        foreign key (CommandId) 
        references [LeadScoringCommand];
GO
    alter table [LeadScoringCommandState] 
        add constraint FKD6AC0B59B951A41C 
        foreign key (CommandId) 
        references [LeadScoringCommand];
GO 