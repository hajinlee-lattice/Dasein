#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

UNAME=`uname`
threshold_version=5.6

# Remove alter table drop foreign key statements from the script
ls $WSHOME/le-db/ddl_globalauthentication_mysql5innodb.sql

if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    mysql_version=$(echo `mysqld --version` | sed 's/[[:alpha:]|(|[:space:]]//g' | cut -d \- -f 1 | cut -d \) -f 1) || 5.5
    sed -i '' 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_globalauthentication_mysql5innodb.sql
else
    echo "You are on ${UNAME}"
    mysql_version=$(echo `mysqld --version` | sed 's/[[:alpha:]|(|[:space:]]//g' | cut -d \- -f 1) || 5.5
    sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_globalauthentication_mysql5innodb.sql
fi


if version_gt ${mysql_version} ${threshold_version}; then
    echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_globalauth.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | mysql -u root -pwelcome

else
    echo "MySQL version $mysql_version"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_globalauth.sql | mysql -u root -pwelcome
fi


