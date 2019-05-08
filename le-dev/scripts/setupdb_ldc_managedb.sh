#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

DDL="$WSHOME/ddl_ldc_managedb_mysql5innodb.sql"
if [ ! -f "${DDL}" ]; then
    mvn -T6 -f $WSHOME/db-pom.xml -DskipTests clean install
fi

UNAME=`uname`
threshold_version=5.6
echo "Setting up LDC_ManagedDB"

if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    sed -i '' 's/alter table .* drop foreign key .*;//g' $DDL
else
    echo "You are on ${UNAME}"
    # Remove alter table drop foreign key statements from the script
    sed -i 's/alter table .* drop foreign key .*;//g' $DDL
fi

# 2.0.6 & 2.0.14 version is needed for some testing purpose, don't remove them
# Besides above 2 versions, most recent 3 versions should be enough
rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn206.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn206.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn206.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2014.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2014.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2014.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2016.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2016.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2016.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2017.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2017.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2017.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2018.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2018.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterColumn2018.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceColumn.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/CategoricalAttribute.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterFact.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterFact.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/AccountMasterFact.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceAttribute.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceAttribute.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/SourceAttribute.csv

rm -rf $WSHOME/le-dev/testartifacts/LDC_ManageDB/CustomerSourceAttribute.csv || true
gunzip -c $WSHOME/le-dev/testartifacts/LDC_ManageDB/CustomerSourceAttribute.csv.gz > $WSHOME/le-dev/testartifacts/LDC_ManageDB/CustomerSourceAttribute.csv

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





