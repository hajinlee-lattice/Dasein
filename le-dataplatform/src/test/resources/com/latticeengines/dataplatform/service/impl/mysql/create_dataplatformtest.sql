CREATE DATABASE dataplatformtest;

USE dataplatformtest;

CREATE TABLE iris (
  ID INT NOT NULL AUTO_INCREMENT,
  SEPAL_LENGTH FLOAT,
  SEPAL_WIDTH FLOAT,
  PETAL_LENGTH FLOAT,
  PETAL_WIDTH FLOAT,
  CATEGORY INT NOT NULL,
  PRIMARY KEY(ID)
);

create table [iris] (
        [ID] bigint int IDENTITY(1,1) PRIMARY KEY,
        [SEPAL_LENGTH] float,
	[SEPAL_WIDTH] float,
	[PETAL_LENGTH] float,
	[PETAL_WIDTH] float,
	[CATEGORY] int not null,
	);

create table [iris_metadata] (
        [QueryForMacro] int not null,
[barecolumnname] nvarchar(max),
[barecolumnvalue] nvarchar(max),
[Dtype] nvarchar(6),
[maxV] float,
[minV] float,
[EventTableName] nvarchar(11),
[TargetEventTableName] nvarchar(11)
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

LOAD DATA INFILE '/home/rgonzalez/workspace/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_train.dat'
INTO TABLE iris 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
(SEPAL_LENGTH, SEPAL_WIDTH, PETAL_LENGTH, PETAL_WIDTH, CATEGORY);

LOAD DATA INFILE '/home/rgonzalez/workspace/ledp/le-dataplatform/src/test/resources/com/latticeengines/dataplatform/service/impl/nn_test.dat'
INTO TABLE iris 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
(SEPAL_LENGTH, SEPAL_WIDTH, PETAL_LENGTH, PETAL_WIDTH, CATEGORY);

COMMIT;
