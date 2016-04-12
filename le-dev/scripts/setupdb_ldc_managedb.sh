#!/bin/bash

# Remove alter table drop foreign key statements from the script
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql

sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | mysql -u root -pwelcome
