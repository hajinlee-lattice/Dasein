DROP SCHEMA IF EXISTS PLS_MultiTenant;

CREATE SCHEMA IF NOT EXISTS PLS_MultiTenant;

GRANT ALL ON PLS_MultiTenant.* TO root@localhost;

USE `PLS_MultiTenant`;

source WSHOME/le-db/ddl_pls_multitenant_mysql5innodb.sql

INSERT INTO QUARTZ_JOBACTIVE(IsActive, JobName, TenantId) VALUES (1, 'modelSummaryDownload', 'PredefinedJobs');
INSERT INTO QUARTZ_JOBACTIVE(IsActive, JobName, TenantId) VALUES (0, 'dataCloudRefresh', 'PredefinedJobs');

INSERT INTO TENANT(
  TENANT_ID,
  NAME,
  REGISTERED_TIME,
  UI_VERSION
) VALUES (
  'LocalTest.LocalTest.Production',
  'LocalTest',
  UNIX_TIMESTAMP(DATE_ADD(NOW(), INTERVAL -1 YEAR)),
  '3.0'
), (
  'PropDataService.PropDataService.Production',
  'PropDataService',
  UNIX_TIMESTAMP(DATE_ADD(NOW(), INTERVAL -1 YEAR)),
  '3.0'
)