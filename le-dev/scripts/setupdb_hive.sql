CREATE USER IF NOT EXISTS 'hiveuser'@'%' IDENTIFIED BY 'hivepassword';
GRANT all on *.* to 'hiveuser'@'%' identified by 'hivepassword';
FLUSH PRIVILEGES;
DROP SCHEMA IF EXISTS `metastore`;
