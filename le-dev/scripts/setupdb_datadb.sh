#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

DDL="$WSHOME/ddl_data_multitenant_mysql5innodb.sql"
if [ ! -f "${DDL}" ]; then
    mvn -T6 -f $WSHOME/db-pom.xml -DskipTests clean install
fi

UNAME=`uname`
threshold_version=5.6
echo "Setting up Data_Multitenant"

if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    # Remove alter table drop foreign key statements from the script, limit FK checks during session
    sed -i '' 's/alter table .* drop foreign key .*;/set @@foreign_key_checks=0;/g' $DDL

else
    echo "You are on ${UNAME}"
    # Remove alter table drop foreign key statements from the script, limit FK checks during session
    sed -i 's/alter table .* drop foreign key .*;/set @@foreign_key_checks=0;/g' $DDL
fi

source $WSHOME/le-dev/scripts/setupdb_parameters.sh

mysql_version=$(mysql --version | sed 's/.*Distrib //' | cut -d , -f 1) || true
if [ -z "${mysql_version}" ]; then
    mysql_version=5.5
fi

if version_gt ${mysql_version} ${threshold_version}; then
    echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_data_multitenant.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | eval $MYSQL_COMMAND

else
    echo "MySQL version $mysql_version"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_data_multitenant.sql | eval $MYSQL_COMMAND
fi

