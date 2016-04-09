DROP DATABASE PLS_MultiTenant;

CREATE DATABASE IF NOT EXISTS PLS_MultiTenant;

GRANT ALL ON PLS_MultiTenant.* TO root;

USE PLS_MultiTenant;

source WSHOME/le-db/ddl_pls_multitenant_mysql5innodb.sql

DROP DATABASE LDC_ManageDB;

CREATE DATABASE IF NOT EXISTS LDC_ManageDB;

GRANT ALL ON LDC_ManageDB.* TO root;

USE LDC_ManageDB;

source WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql

DROP DATABASE LeadScoringDB;

CREATE DATABASE IF NOT EXISTS LeadScoringDB;

GRANT ALL ON LeadScoringDB.* TO root;

USE LeadScoringDB;

source WSHOME/le-dataplatform/ddl_leadscoringdb_mysql.sql
source WSHOME/le-dev/testartifacts/sql/ddl_data_leadscoringdb_mysql5innodb.sql

