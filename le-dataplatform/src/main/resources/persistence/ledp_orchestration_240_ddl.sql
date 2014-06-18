use [LeadScoringDB]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

ALTER TABLE dbo.LeadScoringCommand ADD ModelCommandStep VARCHAR(255) NULL;

IF OBJECT_ID('LeadScoringCommandLog', 'U') IS NOT NULL
    drop table [LeadScoringCommandLog];
GO

IF OBJECT_ID('LeadScoringCommandState', 'U') IS NOT NULL
    drop table [LeadScoringCommandState];
GO

    create table [LeadScoringCommandLog] (
        [PID] int identity not null unique,
        [Created] datetime,
        [Message] [nvarchar](max) not null,
        CommandId int not null,
        primary key ([PID])
    );
GO

    create table [LeadScoringCommandState] (
        [PID] int identity not null unique,
        [Created] datetime,        
        [Diagnostics] [nvarchar](max),
        [ElapsedTimeInMillis] int,
        [ModelCommandStep] varchar(255) not null,
        [Progress] float,
        [Status] varchar(255),
        [TrackingUrl] [nvarchar](max),
        [YarnApplicationId] varchar(255),
        CommandId int not null,
        primary key ([PID])
    );
GO

    alter table [LeadScoringCommandLog] 
        add constraint FK73D5FC0CB951A41C 
        foreign key (CommandId) 
        references [LeadScoringCommand];
GO

    alter table [LeadScoringCommandState] 
        add constraint FKD6AC0B59B951A41C 
        foreign key (CommandId) 
        references [LeadScoringCommand];
GO 

