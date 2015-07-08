CREATE DATABASE LeadScoringDB;

USE LeadScoringDB;

drop table if exists `Q_EventTable_Nutanix`;

drop table if exists `Q_EventTableDepivot_Nutanix`;

drop table if exists `EventMetadata_Nutanix`;

drop table if exists `iris`;

drop table if exists `iris_metadata`;

CREATE TABLE Q_EventTable_Nutanix (
  ID INT NOT NULL AUTO_INCREMENT,
  SEPAL_LENGTH FLOAT,
  SEPAL_WIDTH FLOAT,
  PETAL_LENGTH FLOAT,
  PETAL_WIDTH FLOAT,
  CATEGORY INT NOT NULL,
  PRIMARY KEY(ID)
);

CREATE TABLE Q_EventTableDepivot_Nutanix (
  ID INT NOT NULL AUTO_INCREMENT,
  SEPAL_LENGTH FLOAT,
  SEPAL_WIDTH FLOAT,
  PETAL_LENGTH FLOAT,
  PETAL_WIDTH FLOAT,
  CATEGORY INT NOT NULL,
  PRIMARY KEY(ID)
);

CREATE TABLE Q_EventTableDepivot_Nutanix_FewRows (
  ID INT NOT NULL AUTO_INCREMENT,
  SEPAL_LENGTH FLOAT,
  SEPAL_WIDTH FLOAT,
  PETAL_LENGTH FLOAT,
  PETAL_WIDTH FLOAT,
  CATEGORY INT NOT NULL,
  PRIMARY KEY(ID)
);

CREATE TABLE EventMetadata_Nutanix (
  `QueryForMacro` INT NOT NULL,
  barecolumnname nvarchar(510),
  barecolumnvalue nvarchar(510),
  `Dtype` nvarchar(6),
  `maxV` FLOAT,
  `minV` FLOAT,
  `EventTableName` nvarchar(11),
  `TargetEventTableName` nvarchar(11)
);

CREATE TABLE iris (
  ID INT NOT NULL AUTO_INCREMENT,
  SEPAL_LENGTH FLOAT,
  SEPAL_WIDTH FLOAT,
  PETAL_LENGTH FLOAT,
  PETAL_WIDTH FLOAT,
  CATEGORY INT NOT NULL,
  PRIMARY KEY(ID)
);      

CREATE TABLE iris_metadata (
  `QueryForMacro` INT NOT NULL,
  barecolumnname nvarchar(510),
  barecolumnvalue nvarchar(510),
  `Dtype` nvarchar(6),
  `maxV` FLOAT,
  `minV` FLOAT,
  `EventTableName` nvarchar(11),
  `TargetEventTableName` nvarchar(11)
);

LOAD DATA INFILE '/Users/bnguyen/dev/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_train.dat'
INTO TABLE iris 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
(SEPAL_LENGTH, SEPAL_WIDTH, PETAL_LENGTH, PETAL_WIDTH, CATEGORY);

LOAD DATA INFILE '/Users/bnguyen/dev/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_test.dat'
INTO TABLE iris 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
(SEPAL_LENGTH, SEPAL_WIDTH, PETAL_LENGTH, PETAL_WIDTH, CATEGORY);

insert into EventMetadata_Nutanix select * from iris_metadata;
insert into Q_EventTable_Nutanix select * from iris;
insert into Q_EventTableDepivot_Nutanix select * from iris;
insert into Q_EventTableDepivot_Nutanix_FewRows select * from iris;
alter table Q_EventTable_Nutanix CHANGE ID Nutanix_EventTable_Clean INT;
alter table Q_EventTable_Nutanix CHANGE CATEGORY P1_Event INT;
alter table Q_EventTableDepivot_Nutanix CHANGE ID Nutanix_EventTable_Clean INT;
alter table Q_EventTableDepivot_Nutanix CHANGE CATEGORY P1_Event INT;

insert into EventMetadata_Nutanix (QueryForMacro, barecolumnname, barecolumnvalue, Dtype) VALUES (1,'SEPAL_LENGTH','a','a'),(2,'SEPAL_WIDTH','a','a'),(3,'PETAL_LENGTH','a','a'),(4,'PETAL_WIDTH','a','a');

COMMIT;
