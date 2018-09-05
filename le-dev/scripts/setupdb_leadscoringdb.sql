DROP SCHEMA IF EXISTS LeadScoringDB;

CREATE SCHEMA IF NOT EXISTS LeadScoringDB;

GRANT ALL ON LeadScoringDB.* TO root@localhost;
ALTER DATABASE LeadScoringDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;
USE `LeadScoringDB`;

source WSHOME/ddl_leadscoringdb_mysql5innodb.sql;
