USE ScoringDB;

drop table if exists `TestLeadsTable`;

CREATE TABLE TestLeadsTable (
  Lead_ID INT NOT NULL AUTO_INCREMENT,
  SEPAL_LENGTH FLOAT,
  SEPAL_WIDTH FLOAT,
  PETAL_LENGTH FLOAT,
  PETAL_WIDTH FLOAT,
  CATEGORY INT NOT NULL,
  MODEL_ID nvarchar(510) NOT NULL,
  PRIMARY KEY(Lead_ID)
);

LOAD DATA INFILE '/home/hliu/workspace6/ledp/le-scoring/src/test/resources/com/latticeengines/scoring/mysql/nn_train.dat'
INTO TABLE TestLeadsTable
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
(SEPAL_LENGTH, SEPAL_WIDTH, PETAL_LENGTH, PETAL_WIDTH, CATEGORY, MODEL_ID);

LOAD DATA INFILE '/home/hliu/workspace6/ledp/le-scoring/src/test/resources/com/latticeengines/scoring/mysql/nn_test.dat'
INTO TABLE TestLeadsTable 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
(SEPAL_LENGTH, SEPAL_WIDTH, PETAL_LENGTH, PETAL_WIDTH, CATEGORY, MODEL_ID);

COMMIT;
