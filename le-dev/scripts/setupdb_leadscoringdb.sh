#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

UNAME=`uname`
threshold_version=5.6

if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    # Remove alter table drop foreign key statements from the script
    sed -i '' 's/alter table .* drop foreign key .*;//g' $WSHOME/le-dataplatform/ddl_leadscoringdb_mysql.sql
    mysql_version=$(echo `mysqld --version` | sed 's/[[:alpha:]|(|[:space:]]//g' | cut -d \- -f 1 | cut -d \) -f 1) || 5.5
else
    echo "You are on ${UNAME}"
    # Remove alter table drop foreign key statements from the script
    sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-dataplatform/ddl_leadscoringdb_mysql.sql
    mysql_version=$(echo `mysqld --version` | sed 's/[[:alpha:]|(|[:space:]]//g' | cut -d \- -f 1) || 5.5
fi

if version_gt ${mysql_version} ${threshold_version}; then
    echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_leadscoringdb.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | mysql -u root -pwelcome
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/testartifacts/sql/ddl_data_leadscoringdb_mysql5innodb.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | mysql -u root -pwelcome

else
    echo "MySQL version $mysql_version"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_leadscoringdb.sql | mysql -u root -pwelcome
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/testartifacts/sql/ddl_data_leadscoringdb_mysql5innodb.sql | mysql -u root -pwelcome
fi



