DROP SCHEMA IF EXISTS LeadScoringDB;

CREATE SCHEMA IF NOT EXISTS LeadScoringDB;

GRANT ALL ON LeadScoringDB.* TO root@localhost;

USE `LeadScoringDB`;

source WSHOME/le-dataplatform/ddl_leadscoringdb_mysql.sql;

