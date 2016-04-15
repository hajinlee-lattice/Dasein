#!/bin/bash

# Remove alter table drop foreign key statements from the script
sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/ddl_pls_multitenant_mysql5innodb.sql

sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_pls_multitenant.sql | mysql -u root -pwelcome
