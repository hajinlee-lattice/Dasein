USE [oauth2_dev]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TENANT]') AND type in (N'U'))
DROP TABLE [dbo].[TENANT]
GO
CREATE TABLE [dbo].[TENANT](
    [PID] [bigint] IDENTITY(1,1) NOT NULL,
    [TENANT_NAME] [nvarchar](256) NOT NULL,
    [EXTERNAL_ID] [nvarchar](256) NULL,
    [JDBC_DRIVER] [nvarchar](256) NOT NULL,
    [JDBC_URL] [nvarchar](256) NOT NULL,
    [JDBC_USERNAME] [nvarchar](256) NOT NULL,
    [JDBC_PASSWORD] [nvarchar](256) NOT NULL,
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [tenant_name_idx] ON [dbo].[TENANT] 
([TENANT_NAME] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[authorities]') AND type in (N'U'))
DROP TABLE [dbo].[authorities]
GO
CREATE TABLE [dbo].[authorities](
    [username] [varchar](256) NULL,
    [authority] [varchar](256) NULL
) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [username_idx] ON [dbo].[authorities] 
(
    [username] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[oauth_access_token]') AND type in (N'U'))
DROP TABLE [dbo].[oauth_access_token]
GO
CREATE TABLE [dbo].[oauth_access_token](
    [token_id] [varchar](256) NULL,
    [token] [varbinary](max) NULL,
    [authentication_id] [varchar](256) NULL,
    [user_name] [varchar](256) NULL,
    [client_id] [varchar](256) NULL,
    [authentication] [varbinary](max) NULL,
    [refresh_token] [varchar](256) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
CREATE UNIQUE NONCLUSTERED INDEX [token_id_idx] ON [dbo].[oauth_access_token] 
([token_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [username_idx] ON [dbo].[oauth_access_token] 
([user_name] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [client_id_idx] ON [dbo].[oauth_access_token] 
([client_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [authentication_id_idx] ON [dbo].[oauth_access_token] 
([authentication_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[oauth_client_details]') AND type in (N'U'))
DROP TABLE [dbo].[oauth_client_details]
GO
CREATE TABLE [dbo].[oauth_client_details](
    [client_id] [varchar](256) NOT NULL,
    [resource_ids] [varchar](256) NULL,
    [client_secret] [varchar](256) NULL,
    [scope] [varchar](256) NULL,
    [authorized_grant_types] [varchar](256) NULL,
    [web_server_redirect_uri] [varchar](256) NULL,
    [authorities] [varchar](256) NULL,
    [access_token_validity] [int] NULL,
    [refresh_token_validity] [int] NULL,
    [additional_information] [varchar](4096) NULL,
    [autoapprove] [varchar](256) NULL,
    [client_secret_expiration] [datetime] NULL
) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [client_id_idx] ON [dbo].[oauth_client_details] 
([client_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[oauth_client_token]') AND type in (N'U'))
DROP TABLE [dbo].[oauth_client_token]
GO
CREATE TABLE [dbo].[oauth_client_token](
    [token_id] [varchar](256) NULL,
    [token] [text] NULL,
    [authentication_id] [varchar](256) NULL,
    [user_name] [varchar](256) NULL,
    [client_id] [varchar](256) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
CREATE NONCLUSTERED INDEX [token_id_idx] ON [dbo].[oauth_client_token] 
([token_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [username_idx] ON [dbo].[oauth_client_token] 
([user_name] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO
CREATE NONCLUSTERED INDEX [client_id_idx] ON [dbo].[oauth_client_token] 
([client_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO



IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[oauth_code]') AND type in (N'U'))
DROP TABLE [dbo].[oauth_code]
GO
CREATE TABLE [dbo].[oauth_code](
    [code] [varchar](256) NULL,
    [authentication] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
CREATE NONCLUSTERED INDEX [code_idx] ON [dbo].[oauth_code] 
([code] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[oauth_refresh_token]') AND type in (N'U'))
DROP TABLE [dbo].[oauth_refresh_token]
GO
CREATE TABLE [dbo].[oauth_refresh_token](
    [token_id] [varchar](256) NULL,
    [token] [varbinary](max) NULL,
    [authentication] [varbinary](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
CREATE NONCLUSTERED INDEX [token_id_idx] ON [dbo].[oauth_refresh_token] 
([token_id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[users]') AND type in (N'U'))
DROP TABLE [dbo].[users]
GO
CREATE TABLE [dbo].[users](
    [username] [varchar](256) NULL,
    [password] [varchar](256) NULL,
    [enabled] [bit] NULL
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [username_idx] ON [dbo].[users] 
([username] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
GO

/*
INSERT INTO [dbo].[TENANT]
           ([TENANT_NAME]
           ,[EXTERNAL_ID]
           ,[JDBC_DRIVER]
           ,[JDBC_URL]
           ,[JDBC_USERNAME]
           ,[JDBC_PASSWORD])
     VALUES
           ('playmaker', 'externalId2', 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
           'jdbc:sqlserver://10.41.1.82\sql2012std;databaseName=ADEDTBDd70064747nA26263627n1',
           'playmaker', 'playmaker')
GO

INSERT INTO [dbo].[authorities] ([username], [authority]) VALUES ('marissa', 'ROLE_USER')
GO
*/

INSERT INTO [dbo].[oauth_client_details]
           ([client_id]
           ,[resource_ids]
           ,[client_secret]
           ,[scope]
           ,[authorized_grant_types]
           ,[web_server_redirect_uri]
           ,[authorities]
           ,[access_token_validity]
           ,[refresh_token_validity]
           ,[additional_information]
           ,[autoapprove])
     VALUES
           ('playmaker-admin', 'playmaker_api', 'slk4G111Msd8', 'read,write', 'authorization_code,refresh_token,client_credentials', NULL,
            'ROLE_PLAYMAKER_ADMIN', NULL, NULL, NULL, 'false')
GO
INSERT INTO [dbo].[users]
           ([username]
           ,[password]
           ,[enabled])
     VALUES ('testuser1@latticeengines.com', 'Lattice1',    1)
GO


SET ANSI_PADDING OFF
GO

