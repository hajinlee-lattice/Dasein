DROP SCHEMA IF EXISTS PLS_MultiTenant;
CREATE SCHEMA IF NOT EXISTS PLS_MultiTenant;
GRANT ALL ON PLS_MultiTenant.* TO root@localhost;
USE `PLS_MultiTenant`;

SOURCE WSHOME/ddl_pls_multitenant_mysql5innodb.sql

INSERT INTO TENANT(
TENANT_ID,
NAME,
REGISTERED_TIME,
UI_VERSION
) VALUES (
'LocalTest.LocalTest.Production',
'LocalTest',
UNIX_TIMESTAMP(DATE_ADD(NOW(), INTERVAL -1 YEAR )),
'3.0'
), (
'PropDataService.PropDataService.Production',
'PropDataService',
UNIX_TIMESTAMP(DATE_ADD(NOW(), INTERVAL -1 YEAR )),
'3.0'
), (
'DataCloudService.DataCloudService.Production',
'DataCloudService',
UNIX_TIMESTAMP(DATE_ADD(NOW(), INTERVAL -1 YEAR )),
'3.0'
);

CREATE TABLE WORKFLOW_JOB_INSTANCE (
  JOB_INSTANCE_ID BIGINT       NOT NULL PRIMARY KEY,
  VERSION         BIGINT,
  JOB_NAME        VARCHAR(100) NOT NULL,
  JOB_KEY         VARCHAR(32)  NOT NULL,
  CONSTRAINT JOB_INST_UN UNIQUE (JOB_NAME, JOB_KEY)
)
  ENGINE = InnoDB;

CREATE TABLE WORKFLOW_JOB_EXECUTION (
  JOB_EXECUTION_ID           BIGINT        NOT NULL PRIMARY KEY,
  VERSION                    BIGINT,
  JOB_INSTANCE_ID            BIGINT        NOT NULL,
  CREATE_TIME                DATETIME      NOT NULL,
  START_TIME                 DATETIME DEFAULT NULL,
  END_TIME                   DATETIME DEFAULT NULL,
  STATUS                     VARCHAR(10),
  EXIT_CODE                  VARCHAR(2500),
  EXIT_MESSAGE               VARCHAR(2500),
  LAST_UPDATED               DATETIME,
  JOB_CONFIGURATION_LOCATION VARCHAR(2500) NULL,
  CONSTRAINT JOB_INST_EXEC_FK FOREIGN KEY (JOB_INSTANCE_ID)
  REFERENCES WORKFLOW_JOB_INSTANCE (JOB_INSTANCE_ID)
)
  ENGINE = InnoDB;

CREATE TABLE WORKFLOW_JOB_EXECUTION_PARAMS (
  JOB_EXECUTION_ID BIGINT     NOT NULL,
  TYPE_CD          VARCHAR(6) NOT NULL,
  KEY_NAME         TEXT       NOT NULL,
  STRING_VAL       MEDIUMTEXT,
  DATE_VAL         DATETIME DEFAULT NULL,
  LONG_VAL         BIGINT,
  DOUBLE_VAL       DOUBLE PRECISION,
  IDENTIFYING      CHAR(1)    NOT NULL,
  CONSTRAINT JOB_EXEC_PARAMS_FK FOREIGN KEY (JOB_EXECUTION_ID)
  REFERENCES WORKFLOW_JOB_EXECUTION (JOB_EXECUTION_ID)
)
  ENGINE = InnoDB;

CREATE TABLE WORKFLOW_STEP_EXECUTION (
  STEP_EXECUTION_ID  BIGINT       NOT NULL PRIMARY KEY,
  VERSION            BIGINT       NOT NULL,
  STEP_NAME          VARCHAR(100) NOT NULL,
  JOB_EXECUTION_ID   BIGINT       NOT NULL,
  START_TIME         DATETIME     NOT NULL,
  END_TIME           DATETIME DEFAULT NULL,
  STATUS             VARCHAR(10),
  COMMIT_COUNT       BIGINT,
  READ_COUNT         BIGINT,
  FILTER_COUNT       BIGINT,
  WRITE_COUNT        BIGINT,
  READ_SKIP_COUNT    BIGINT,
  WRITE_SKIP_COUNT   BIGINT,
  PROCESS_SKIP_COUNT BIGINT,
  ROLLBACK_COUNT     BIGINT,
  EXIT_CODE          VARCHAR(2500),
  EXIT_MESSAGE       VARCHAR(2500),
  LAST_UPDATED       DATETIME,
  CONSTRAINT JOB_EXEC_STEP_FK FOREIGN KEY (JOB_EXECUTION_ID)
  REFERENCES WORKFLOW_JOB_EXECUTION (JOB_EXECUTION_ID)
)
  ENGINE = InnoDB;

CREATE TABLE WORKFLOW_STEP_EXECUTION_CONTEXT (
  STEP_EXECUTION_ID  BIGINT        NOT NULL PRIMARY KEY,
  SHORT_CONTEXT      VARCHAR(2500) NOT NULL,
  SERIALIZED_CONTEXT TEXT,
  CONSTRAINT STEP_EXEC_CTX_FK FOREIGN KEY (STEP_EXECUTION_ID)
  REFERENCES WORKFLOW_STEP_EXECUTION (STEP_EXECUTION_ID)
)
  ENGINE = InnoDB;

CREATE TABLE WORKFLOW_JOB_EXECUTION_CONTEXT (
  JOB_EXECUTION_ID   BIGINT        NOT NULL PRIMARY KEY,
  SHORT_CONTEXT      VARCHAR(2500) NOT NULL,
  SERIALIZED_CONTEXT MEDIUMTEXT,
  CONSTRAINT JOB_EXEC_CTX_FK FOREIGN KEY (JOB_EXECUTION_ID)
  REFERENCES WORKFLOW_JOB_EXECUTION (JOB_EXECUTION_ID)
)
  ENGINE = InnoDB;

CREATE TABLE WORKFLOW_STEP_EXECUTION_SEQ (
  ID         BIGINT  NOT NULL,
  UNIQUE_KEY CHAR(1) NOT NULL,
  CONSTRAINT UNIQUE_KEY_UN UNIQUE (UNIQUE_KEY)
)
  ENGINE = InnoDB;

INSERT INTO WORKFLOW_STEP_EXECUTION_SEQ (ID, UNIQUE_KEY) SELECT *
                                                         FROM (SELECT
                                                                 0   AS ID,
                                                                 '0' AS UNIQUE_KEY) AS tmp
                                                         WHERE NOT exists(SELECT *
                                                                          FROM WORKFLOW_STEP_EXECUTION_SEQ);

CREATE TABLE WORKFLOW_JOB_EXECUTION_SEQ (
  ID         BIGINT  NOT NULL,
  UNIQUE_KEY CHAR(1) NOT NULL,
  CONSTRAINT UNIQUE_KEY_UN UNIQUE (UNIQUE_KEY)
)
  ENGINE = InnoDB;

INSERT INTO WORKFLOW_JOB_EXECUTION_SEQ (ID, UNIQUE_KEY) SELECT *
                                                        FROM (SELECT
                                                                0   AS ID,
                                                                '0' AS UNIQUE_KEY) AS tmp
                                                        WHERE NOT exists(SELECT *
                                                                         FROM WORKFLOW_JOB_EXECUTION_SEQ);

CREATE TABLE WORKFLOW_JOB_SEQ (
  ID         BIGINT  NOT NULL,
  UNIQUE_KEY CHAR(1) NOT NULL,
  CONSTRAINT UNIQUE_KEY_UN UNIQUE (UNIQUE_KEY)
)
  ENGINE = InnoDB;

INSERT INTO WORKFLOW_JOB_SEQ (ID, UNIQUE_KEY) SELECT *
                                              FROM (SELECT
                                                      0   AS ID,
                                                      '0' AS UNIQUE_KEY) AS tmp
                                              WHERE NOT exists(SELECT *
                                                               FROM WORKFLOW_JOB_SEQ);

COMMIT;