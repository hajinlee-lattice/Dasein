DROP SCHEMA IF EXISTS oauth2DB;

CREATE SCHEMA IF NOT EXISTS oauth2DB;

ALTER DATABASE oauth2DB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

source WSHOME/le-oauth2db/src/main/schema/schema_mysql.sql

