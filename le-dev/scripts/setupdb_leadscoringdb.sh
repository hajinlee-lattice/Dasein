#!/bin/bash

# Remove alter table drop foreign key statements from the script
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-dataplatform/ddl_leadscoringdb_mysql.sql

sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/testartifacts/sql/ddl_data_leadscoringdb_mysql5innodb.sql
sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_leadscoringdb.sql | mysql -u root -pwelcome
