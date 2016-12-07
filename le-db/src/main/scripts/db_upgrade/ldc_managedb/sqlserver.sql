USE [LDC_ManageDB]
GO

IF NOT EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_NAME = 'CategoricalAttribute')
  BEGIN
    CREATE TABLE [CategoricalAttribute] (
      [PID]       BIGINT IDENTITY NOT NULL UNIQUE,
      [AttrName]  NVARCHAR(100)   NOT NULL,
      [AttrValue] NVARCHAR(500)  NOT NULL,
      [ParentID]  BIGINT,
      PRIMARY KEY ([PID]),
      UNIQUE ([AttrName], [AttrValue], [ParentID])
    );
  END
GO

IF NOT EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_NAME = 'AccountMasterFact')
  BEGIN
    CREATE TABLE [AccountMasterFact] (
      [PID]         BIGINT IDENTITY NOT NULL UNIQUE,
      [Category]    BIGINT          NOT NULL,
      [EncodedCube] NVARCHAR(MAX)   NOT NULL,
      [Industry]    BIGINT          NOT NULL,
      [Location]    BIGINT          NOT NULL,
      [NumEmpRange] BIGINT          NOT NULL,
      [NumLocRange] BIGINT          NOT NULL,
      [RevRange]    BIGINT          NOT NULL,
      PRIMARY KEY ([PID]),
      UNIQUE ([Location], [Industry], [NumEmpRange], [RevRange], [NumLocRange], [Category])
    );
  END
GO

IF NOT EXISTS(SELECT *
              FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_NAME = 'CategoricalDimension')
  BEGIN
    CREATE TABLE [CategoricalDimension] (
      [PID]        BIGINT IDENTITY NOT NULL UNIQUE,
      [Dimension]  NVARCHAR(100)   NOT NULL,
      [RootAttrId] BIGINT          NOT NULL UNIQUE,
      [Source]     NVARCHAR(100)   NOT NULL,
      PRIMARY KEY ([PID]),
      UNIQUE ([Source], [Dimension])
    );
  END
GO

IF EXISTS(SELECT *
          FROM sys.indexes
          WHERE name = 'IX_PARENT_ID' AND object_id = OBJECT_ID('CategoricalAttribute'))
  BEGIN
    DROP INDEX IX_PARENT_ID
      ON [CategoricalAttribute]
  END
CREATE INDEX IX_PARENT_ID
  ON [CategoricalAttribute] ([ParentID]);
GO

IF EXISTS(SELECT *
          FROM sys.indexes
          WHERE name = 'IX_DIMENSIONS' AND object_id = OBJECT_ID('AccountMasterFact'))
  BEGIN
    DROP INDEX IX_DIMENSIONS
      ON [AccountMasterFact]
  END
CREATE INDEX IX_DIMENSIONS
  ON [AccountMasterFact] ([Category], [Industry], [Location], [NumEmpRange], [NumLocRange], [RevRange]);
GO

IF EXISTS(SELECT *
          FROM sys.indexes
          WHERE name = 'IX_SOURCE_DIMENSION' AND object_id = OBJECT_ID('CategoricalDimension'))
  BEGIN
    DROP INDEX IX_SOURCE_DIMENSION
      ON [CategoricalDimension]
  END
CREATE INDEX IX_SOURCE_DIMENSION
  ON [CategoricalDimension] ([Dimension], [Source]);
GO