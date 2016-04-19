#!/usr/bin/env bash

# Remove alter table drop foreign key statements from the script
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-scoring/ddl_scoringdb_mysql5innodb.sql
sed -i 's/varchar(65535)/varchar(4000)/g' $WSHOME/le-scoring/ddl_scoringdb_mysql5innodb.sql

sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_scoringdb.sql | mysql -u root -pwelcome