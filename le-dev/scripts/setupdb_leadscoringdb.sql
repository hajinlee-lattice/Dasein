DROP DATABASE LeadScoringDB;

CREATE DATABASE IF NOT EXISTS LeadScoringDB;

GRANT ALL ON LeadScoringDB.* TO root;

USE LeadScoringDB;

source WSHOME/le-dataplatform/ddl_leadscoringdb_mysql.sql
source WSHOME/le-dev/testartifacts/sql/ddl_data_leadscoringdb_mysql5innodb.sql

