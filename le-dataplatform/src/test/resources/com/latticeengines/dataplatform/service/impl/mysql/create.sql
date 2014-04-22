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