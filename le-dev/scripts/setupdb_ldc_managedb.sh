#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

UNAME=`uname`
threshold_version=5.6

if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    sed -i '' 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql
else
    echo "You are on ${UNAME}"
    # Remove alter table drop foreign key statements from the script
    sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-db/ddl_ldc_managedb_mysql5innodb.sql
fi

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv

mysql_version=$(echo `mysqld --version` | sed 's/[[:alpha:]|(|[:space:]]//g' | cut -d \- -f 1 | cut -d \) -f 1) || 5.5
if version_gt ${mysql_version} ${threshold_version}; then
    echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | eval $MYSQL_COMMAND
else
    echo "MySQL version $mysql_version"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | eval $MYSQL_COMMAND
fi





