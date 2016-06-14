IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE
            id = OBJECT_ID(N'[dbo].[FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS]') AND OBJECTPROPERTY(id, N'ISFOREIGNKEY') = 1)
  ALTER TABLE [dbo].[QRTZ_TRIGGERS]
    DROP CONSTRAINT FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE
            id = OBJECT_ID(N'[dbo].[FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS]') AND OBJECTPROPERTY(id, N'ISFOREIGNKEY') = 1)
  ALTER TABLE [dbo].[QRTZ_CRON_TRIGGERS]
    DROP CONSTRAINT FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS]') AND
                OBJECTPROPERTY(id, N'ISFOREIGNKEY') = 1)
  ALTER TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS]
    DROP CONSTRAINT FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS]') AND
                OBJECTPROPERTY(id, N'ISFOREIGNKEY') = 1)
  ALTER TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS]
    DROP CONSTRAINT FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_CALENDARS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_CALENDARS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_CRON_TRIGGERS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_CRON_TRIGGERS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_BLOB_TRIGGERS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_BLOB_TRIGGERS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_FIRED_TRIGGERS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_FIRED_TRIGGERS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_PAUSED_TRIGGER_GRPS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_PAUSED_TRIGGER_GRPS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_SCHEDULER_STATE]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_SCHEDULER_STATE]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_LOCKS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_LOCKS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_JOB_DETAILS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_JOB_DETAILS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_SIMPLE_TRIGGERS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_SIMPROP_TRIGGERS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS]
GO

IF EXISTS(SELECT *
          FROM dbo.sysobjects
          WHERE id = OBJECT_ID(N'[dbo].[QRTZ_TRIGGERS]') AND OBJECTPROPERTY(id, N'ISUSERTABLE') = 1)
  DROP TABLE [dbo].[QRTZ_TRIGGERS]
GO

CREATE TABLE [dbo].[QRTZ_CALENDARS] (
  [SCHED_NAME]    [VARCHAR](120) NOT NULL,
  [CALENDAR_NAME] [VARCHAR](200) NOT NULL,
  [CALENDAR]      [IMAGE]        NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_CRON_TRIGGERS] (
  [SCHED_NAME]      [VARCHAR](120) NOT NULL,
  [TRIGGER_NAME]    [VARCHAR](200) NOT NULL,
  [TRIGGER_GROUP]   [VARCHAR](200) NOT NULL,
  [CRON_EXPRESSION] [VARCHAR](120) NOT NULL,
  [TIME_ZONE_ID]    [VARCHAR](80)
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_FIRED_TRIGGERS] (
  [SCHED_NAME]        [VARCHAR](120) NOT NULL,
  [ENTRY_ID]          [VARCHAR](95)  NOT NULL,
  [TRIGGER_NAME]      [VARCHAR](200) NOT NULL,
  [TRIGGER_GROUP]     [VARCHAR](200) NOT NULL,
  [INSTANCE_NAME]     [VARCHAR](200) NOT NULL,
  [FIRED_TIME]        [BIGINT]       NOT NULL,
  [SCHED_TIME]        [BIGINT]       NOT NULL,
  [PRIORITY]          [INTEGER]      NOT NULL,
  [STATE]             [VARCHAR](16)  NOT NULL,
  [JOB_NAME]          [VARCHAR](200) NULL,
  [JOB_GROUP]         [VARCHAR](200) NULL,
  [IS_NONCONCURRENT]  [VARCHAR](1)   NULL,
  [REQUESTS_RECOVERY] [VARCHAR](1)   NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_PAUSED_TRIGGER_GRPS] (
  [SCHED_NAME]    [VARCHAR](120) NOT NULL,
  [TRIGGER_GROUP] [VARCHAR](200) NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_SCHEDULER_STATE] (
  [SCHED_NAME]        [VARCHAR](120) NOT NULL,
  [INSTANCE_NAME]     [VARCHAR](200) NOT NULL,
  [LAST_CHECKIN_TIME] [BIGINT]       NOT NULL,
  [CHECKIN_INTERVAL]  [BIGINT]       NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_LOCKS] (
  [SCHED_NAME] [VARCHAR](120) NOT NULL,
  [LOCK_NAME]  [VARCHAR](40)  NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_JOB_DETAILS] (
  [SCHED_NAME]        [VARCHAR](120) NOT NULL,
  [JOB_NAME]          [VARCHAR](200) NOT NULL,
  [JOB_GROUP]         [VARCHAR](200) NOT NULL,
  [DESCRIPTION]       [VARCHAR](250) NULL,
  [JOB_CLASS_NAME]    [VARCHAR](250) NOT NULL,
  [IS_DURABLE]        [VARCHAR](1)   NOT NULL,
  [IS_NONCONCURRENT]  [VARCHAR](1)   NOT NULL,
  [IS_UPDATE_DATA]    [VARCHAR](1)   NOT NULL,
  [REQUESTS_RECOVERY] [VARCHAR](1)   NOT NULL,
  [JOB_DATA]          [IMAGE]        NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS] (
  [SCHED_NAME]      [VARCHAR](120) NOT NULL,
  [TRIGGER_NAME]    [VARCHAR](200) NOT NULL,
  [TRIGGER_GROUP]   [VARCHAR](200) NOT NULL,
  [REPEAT_COUNT]    [BIGINT]       NOT NULL,
  [REPEAT_INTERVAL] [BIGINT]       NOT NULL,
  [TIMES_TRIGGERED] [BIGINT]       NOT NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS] (
  [SCHED_NAME]    [VARCHAR](120)   NOT NULL,
  [TRIGGER_NAME]  [VARCHAR](200)   NOT NULL,
  [TRIGGER_GROUP] [VARCHAR](200)   NOT NULL,
  [STR_PROP_1]    [VARCHAR](512)   NULL,
  [STR_PROP_2]    [VARCHAR](512)   NULL,
  [STR_PROP_3]    [VARCHAR](512)   NULL,
  [INT_PROP_1]    [INT]            NULL,
  [INT_PROP_2]    [INT]            NULL,
  [LONG_PROP_1]   [BIGINT]         NULL,
  [LONG_PROP_2]   [BIGINT]         NULL,
  [DEC_PROP_1]    [NUMERIC](13, 4) NULL,
  [DEC_PROP_2]    [NUMERIC](13, 4) NULL,
  [BOOL_PROP_1]   [VARCHAR](1)     NULL,
  [BOOL_PROP_2]   [VARCHAR](1)     NULL,
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_BLOB_TRIGGERS] (
  [SCHED_NAME]    [VARCHAR](120) NOT NULL,
  [TRIGGER_NAME]  [VARCHAR](200) NOT NULL,
  [TRIGGER_GROUP] [VARCHAR](200) NOT NULL,
  [BLOB_DATA]     [IMAGE]        NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QRTZ_TRIGGERS] (
  [SCHED_NAME]     [VARCHAR](120) NOT NULL,
  [TRIGGER_NAME]   [VARCHAR](200) NOT NULL,
  [TRIGGER_GROUP]  [VARCHAR](200) NOT NULL,
  [JOB_NAME]       [VARCHAR](200) NOT NULL,
  [JOB_GROUP]      [VARCHAR](200) NOT NULL,
  [DESCRIPTION]    [VARCHAR](250) NULL,
  [NEXT_FIRE_TIME] [BIGINT]       NULL,
  [PREV_FIRE_TIME] [BIGINT]       NULL,
  [PRIORITY]       [INTEGER]      NULL,
  [TRIGGER_STATE]  [VARCHAR](16)  NOT NULL,
  [TRIGGER_TYPE]   [VARCHAR](8)   NOT NULL,
  [START_TIME]     [BIGINT]       NOT NULL,
  [END_TIME]       [BIGINT]       NULL,
  [CALENDAR_NAME]  [VARCHAR](200) NULL,
  [MISFIRE_INSTR]  [SMALLINT]     NULL,
  [JOB_DATA]       [IMAGE]        NULL
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[JOB_HISTORY] (
  [PID]                BIGINT IDENTITY NOT NULL UNIQUE,
  [JobName]            VARCHAR(255)    NOT NULL,
  [TenantId]           VARCHAR(255)    NOT NULL,
  [TriggeredJobHandle] VARCHAR(255),
  [TriggeredTime]      DATETIME2       NOT NULL,
  [TriggeredJobStatus] INTEGER         NOT NULL,
  [ErrorMessage]       VARCHAR(8000),
  [ExecutionHost]      VARCHAR(8000),
  PRIMARY KEY ([PID])
)
GO

CREATE TABLE [dbo].[QUARTZ_ACTIVESTACK] (
  [PID]         BIGINT IDENTITY NOT NULL UNIQUE,
  [ActiveStack] VARCHAR(255)    NOT NULL,
  PRIMARY KEY ([PID])
)
GO

CREATE TABLE [dbo].[QUARTZ_JOBACTIVE] (
  [PID]      BIGINT IDENTITY NOT NULL UNIQUE,
  [JobName]  VARCHAR(255)    NOT NULL,
  [TenantId] VARCHAR(255)    NOT NULL,
  [IsActive] BIT             NOT NULL,
  PRIMARY KEY ([PID])
)
GO

ALTER TABLE [dbo].[QRTZ_CALENDARS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_CALENDARS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [CALENDAR_NAME]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_CRON_TRIGGERS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_CRON_TRIGGERS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [TRIGGER_NAME],
      [TRIGGER_GROUP]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_FIRED_TRIGGERS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_FIRED_TRIGGERS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [ENTRY_ID]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_PAUSED_TRIGGER_GRPS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_PAUSED_TRIGGER_GRPS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [TRIGGER_GROUP]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_SCHEDULER_STATE]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_SCHEDULER_STATE] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [INSTANCE_NAME]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_LOCKS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_LOCKS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [LOCK_NAME]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_JOB_DETAILS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_JOB_DETAILS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [JOB_NAME],
      [JOB_GROUP]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_SIMPLE_TRIGGERS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [TRIGGER_NAME],
      [TRIGGER_GROUP]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_SIMPROP_TRIGGERS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [TRIGGER_NAME],
      [TRIGGER_GROUP]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_TRIGGERS]
  WITH NOCHECK ADD
  CONSTRAINT [PK_QRTZ_TRIGGERS] PRIMARY KEY CLUSTERED
    (
      [SCHED_NAME],
      [TRIGGER_NAME],
      [TRIGGER_GROUP]
    )
    ON [PRIMARY]
GO

ALTER TABLE [dbo].[QRTZ_CRON_TRIGGERS]
  ADD
  CONSTRAINT [FK_QRTZ_CRON_TRIGGERS_QRTZ_TRIGGERS] FOREIGN KEY
    (
      [SCHED_NAME],
      [TRIGGER_NAME],
      [TRIGGER_GROUP]
    ) REFERENCES [dbo].[QRTZ_TRIGGERS] (
    [SCHED_NAME],
    [TRIGGER_NAME],
    [TRIGGER_GROUP]
  )
    ON DELETE CASCADE
GO

ALTER TABLE [dbo].[QRTZ_SIMPLE_TRIGGERS]
  ADD
  CONSTRAINT [FK_QRTZ_SIMPLE_TRIGGERS_QRTZ_TRIGGERS] FOREIGN KEY
    (
      [SCHED_NAME],
      [TRIGGER_NAME],
      [TRIGGER_GROUP]
    ) REFERENCES [dbo].[QRTZ_TRIGGERS] (
    [SCHED_NAME],
    [TRIGGER_NAME],
    [TRIGGER_GROUP]
  )
    ON DELETE CASCADE
GO

ALTER TABLE [dbo].[QRTZ_SIMPROP_TRIGGERS]
  ADD
  CONSTRAINT [FK_QRTZ_SIMPROP_TRIGGERS_QRTZ_TRIGGERS] FOREIGN KEY
    (
      [SCHED_NAME],
      [TRIGGER_NAME],
      [TRIGGER_GROUP]
    ) REFERENCES [dbo].[QRTZ_TRIGGERS] (
    [SCHED_NAME],
    [TRIGGER_NAME],
    [TRIGGER_GROUP]
  )
    ON DELETE CASCADE
GO

ALTER TABLE [dbo].[QRTZ_TRIGGERS]
  ADD
  CONSTRAINT [FK_QRTZ_TRIGGERS_QRTZ_JOB_DETAILS] FOREIGN KEY
    (
      [SCHED_NAME],
      [JOB_NAME],
      [JOB_GROUP]
    ) REFERENCES [dbo].[QRTZ_JOB_DETAILS] (
    [SCHED_NAME],
    [JOB_NAME],
    [JOB_GROUP]
  )
GO

