#!/bin/bash

# Test for required env variables
printf "%s\n" "${WSHOME:?You must set WSHOME}"

sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_pls_multitenant_mysql5innodb.sql
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql

sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb.sql | mysql -u root -pwelcome
