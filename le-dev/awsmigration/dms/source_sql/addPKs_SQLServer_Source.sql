ALTER TABLE [PLS_MultiTenant].[dbo].[WORKFLOW_JOB_EXECUTION_PARAMS] ADD [PID] bigint identity not null unique;
ALTER TABLE [PLS_MultiTenant].[dbo].[WORKFLOW_JOB_EXECUTION_PARAMS] ADD CONSTRAINT PK_WORKFLOW_JOB_EXECUTION_PARAMS PRIMARY KEY(PID);

ALTER TABLE [oauth2].[dbo].[oauth_access_token] ADD [PID] bigint identity not null unique;
ALTER TABLE [oauth2].[dbo].[oauth_access_token] ADD CONSTRAINT PK_oauth_access_token PRIMARY KEY(PID);

ALTER TABLE [oauth2].[dbo].[oauth_client_token] ADD [PID] bigint identity not null unique;
ALTER TABLE [oauth2].[dbo].[oauth_client_token] ADD CONSTRAINT PK_oauth_client_token PRIMARY KEY(PID);

ALTER TABLE [oauth2].[dbo].[oauth_code] ADD [PID] bigint identity not null unique;
ALTER TABLE [oauth2].[dbo].[oauth_code] ADD CONSTRAINT PK_oauth_code PRIMARY KEY(PID);

ALTER TABLE [oauth2].[dbo].[oauth_refresh_token] ADD [PID] bigint identity not null unique;
ALTER TABLE [oauth2].[dbo].[oauth_refresh_token] ADD CONSTRAINT PK_oauth_refresh_token PRIMARY KEY(PID);
