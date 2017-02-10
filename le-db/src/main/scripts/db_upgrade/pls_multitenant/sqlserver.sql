USE [PLS_MultiTenant]

ALTER TABLE [METADATA_TABLE] ADD [NAMESPACE] NVARCHAR(255);

CREATE TABLE [METADATA_TABLE_TAG] (
  [PID]       BIGINT IDENTITY NOT NULL UNIQUE, 
  [NAME]      NVARCHAR(255)   NOT NULL, 
  [TENANT_ID] BIGINT          NOT NULL, 
  FK_TABLE_ID BIGINT          NOT NULL, 
  PRIMARY KEY ([PID]));

ALTER TABLE [METADATA_TABLE_TAG]
  ADD CONSTRAINT FK6BACA6595FC50F27 FOREIGN KEY (FK_TABLE_ID) REFERENCES [METADATA_TABLE] ON DELETE CASCADE;

CREATE NONCLUSTERED INDEX FK6BACA6595FC50F27 ON [METADATA_TABLE_TAG] (FK_TABLE_ID);

CREATE TABLE [METADATA_STORAGE] (
  [NAME]          NVARCHAR(31)    NOT NULL, 
  [PID]           BIGINT IDENTITY NOT NULL UNIQUE, 
  [TABLE_NAME]    NVARCHAR(255)   NOT NULL, 
  [DATABASE_NAME] INT, 
  FK_TABLE_ID     BIGINT          NOT NULL, 
  PRIMARY KEY ([PID]));

ALTER TABLE [METADATA_STORAGE] 
  ADD CONSTRAINT FKAAD4414B5FC50F27 FOREIGN KEY (FK_TABLE_ID) REFERENCES [METADATA_TABLE] ON DELETE CASCADE;
  
CREATE NONCLUSTERED INDEX FKAAD4414B5FC50F27 ON [METADATA_STORAGE] (FK_TABLE_ID);

INSERT INTO [METADATA_STORAGE] (NAME, TABLE_NAME, FK_TABLE_ID)
SELECT 'HDFS', NAME, PID FROM [METADATA_TABLE];

CREATE TABLE [BUCKET_METADATA] (
    [PID] BIGINT IDENTITY NOT NULL UNIQUE,
    [NAME] NVARCHAR(255) NOT NULL,
    [CREATION_TIMESTAMP] BIGINT NOT NULL,
    [LEFT_BOUND_SCORE] INT NOT NULL,
    [LIFT] DOUBLE PRECISION NOT NULL,
    [MODEL_ID] NVARCHAR(255) NOT NULL,
    [NUM_LEADS] INT NOT NULL,
    [RIGHT_BOUND_SCORE] INT NOT NULL,
    [TENANT_ID] BIGINT NOT NULL,
    FK_TENANT_ID BIGINT NOT NULL,
    PRIMARY KEY ([PID]));

ALTER TABLE [BUCKET_METADATA]
  ADD CONSTRAINT FK398165E436865BC FOREIGN KEY (FK_TENANT_ID) REFERENCES [TENANT] ON DELETE CASCADE;
  
CREATE NONCLUSTERED INDEX FK398165E436865BC ON [BUCKET_METADATA] (FK_TENANT_ID)

IF NOT EXISTS(
  SELECT *
  FROM sys.columns
  WHERE [name] = N'TYPE' AND [object_id] = OBJECT_ID(N'MODELQUALITY_ALGORITHM'))
BEGIN
  ALTER TABLE [MODELQUALITY_ALGORITHM] ADD [TYPE] INT NULL DEFAULT NULL;
END

IF NOT EXISTS(
  SELECT *
  FROM sys.columns
  WHERE [name] = N'CROSS_VALIDATION_MEAN' AND [object_id] = OBJECT_ID(N'MODEL_SUMMARY'))
BEGIN
  ALTER TABLE [MODEL_SUMMARY] ADD [CROSS_VALIDATION_MEAN] FLOAT NULL DEFAULT NULL;
END

IF NOT EXISTS(
  SELECT *
  FROM sys.columns
  WHERE [name] = N'CROSS_VALIDATION_STD' AND [object_id] = OBJECT_ID(N'MODEL_SUMMARY'))
BEGIN
  ALTER TABLE [MODEL_SUMMARY] ADD [CROSS_VALIDATION_STD] FLOAT NULL DEFAULT NULL;
END

IF NOT EXISTS(
  SELECT *
  FROM sys.indexes
  WHERE name = N'IX_MARK_TIME')
BEGIN
  CREATE INDEX IX_MARK_TIME on [MODEL_SUMMARY_DOWNLOAD_FLAGS] ([MARK_TIME]);
END

IF NOT EXISTS(
  SELECT *
  FROM sys.indexes
  WHERE name = N'IX_TENANT_ID')
BEGIN
  CREATE INDEX IX_TENANT_ID on [MODEL_SUMMARY_DOWNLOAD_FLAGS] ([Tenant_ID]);
END

IF NOT EXISTS(
  SELECT *
  FROM sys.columns
  WHERE [object_id] = OBJECT_ID(N'ATTRIBUTE_CUSTOMIZATION_PROPERTY'))
  BEGIN
    create table [ATTRIBUTE_CUSTOMIZATION_PROPERTY] ([PID] bigint identity not null unique, [ATTRIBUTE_NAME] nvarchar(100) not null, [CATEGORY_NAME] nvarchar(255) not null, [PROPERTY_NAME] nvarchar(255) not null, [PROPERTY_VALUE] nvarchar(255), [TENANT_PID] bigint not null, [USE_CASE] nvarchar(255) not null, FK_TENANT_ID bigint not null, primary key ([PID]), unique ([TENANT_PID], [ATTRIBUTE_NAME], [USE_CASE], [CATEGORY_NAME], [PROPERTY_NAME]));
    create index IX_TENANT_ATTRIBUTENAME_USECASE on [ATTRIBUTE_CUSTOMIZATION_PROPERTY] ([ATTRIBUTE_NAME], FK_TENANT_ID, [USE_CASE]);
    alter table [ATTRIBUTE_CUSTOMIZATION_PROPERTY] add constraint FKBFFD520436865BC foreign key (FK_TENANT_ID) references [TENANT] on delete cascade;
    create nonclustered index FKBFFD520436865BC on [ATTRIBUTE_CUSTOMIZATION_PROPERTY] (FK_TENANT_ID)
END

IF NOT EXISTS(
  SELECT *
  FROM sys.columns
  WHERE [object_id] = OBJECT_ID(N'CATEGORY_CUSTOMIZATION_PROPERTY'))
  BEGIN
    create table [CATEGORY_CUSTOMIZATION_PROPERTY] ([PID] bigint identity not null unique, [CATEGORY_NAME] nvarchar(255) not null, [PROPERTY_NAME] nvarchar(255) not null, [PROPERTY_VALUE] nvarchar(255), [TENANT_PID] bigint not null, [USE_CASE] nvarchar(255) not null, FK_TENANT_ID bigint not null, primary key ([PID]), unique ([TENANT_PID], [USE_CASE], [CATEGORY_NAME], [PROPERTY_NAME]));
    create index IX_TENANT_ATTRIBUTECATEGORY_USECASE on [CATEGORY_CUSTOMIZATION_PROPERTY] (FK_TENANT_ID, [USE_CASE]);
    alter table [CATEGORY_CUSTOMIZATION_PROPERTY] add constraint FK60EAD4236865BC foreign key (FK_TENANT_ID) references [TENANT] on delete cascade;
    create nonclustered index FK60EAD4236865BC on [CATEGORY_CUSTOMIZATION_PROPERTY] (FK_TENANT_ID)
END

GO

