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

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn200.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn200.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn200.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn201.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn201.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn201.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn202.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn202.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn202.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn203.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn203.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn203.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn204.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn204.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn204.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterFact.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterFact.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterFact.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceAttribute.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceAttribute.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceAttribute.csv

source $WSHOME/le-dev/scripts/setupdb_parameters.sh

mysql_version=$(mysql --version | sed 's/.*Distrib //' | cut -d , -f 1) || true
if [ -z "${mysql_version}" ]; then
    mysql_version=5.5
fi
if version_gt ${mysql_version} ${threshold_version}; then
    echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | eval $MYSQL_COMMAND
else
    echo "MySQL version $mysql_version"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | eval $MYSQL_COMMAND
fi





