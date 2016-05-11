#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" != "$1"; }

UNAME=`uname`
threshold_version=5.7

if [[ "${UNAME}" == 'Darwin' ]]; then
    echo 'You are on Mac'

    sed -i '' 's/alter table .* drop foreign key .*;//g' $WSHOME/le-propdata/ddl_ldc_managedb_mysql5innodb.sql
    mysql_version=$(echo `mysqld --version` | sed 's/[[:alpha:]|(|[:space:]]//g' | cut -d \- -f 1 | cut -d \) -f 1) || 5.5

    if version_gt ${mysql_version} ${threshold_version}; then
        echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
        sed '' "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | sed "s|DATA|DATA LOCAL|g" | mysql -u root -pwelcome
    else
        echo "MySQL version $mysql_version"
        sed '' "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | mysql -u root -pwelcome
    fi

else

    echo 'You are on ${UNAME}'

    # Remove alter table drop foreign key statements from the script
    sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-propdata/ddl_ldc_managedb_mysql5innodb.sql
    mysql_version=$(echo `mysqld --version` | sed 's/[[:alpha:]|(|[:space:]]//g' | cut -d \- -f 1) || 5.5

    if version_gt ${mysql_version} ${threshold_version}; then
        echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
        sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | sed "s|DATA|DATA LOCAL|g" | mysql -u root -pwelcome
    else
        echo "MySQL version $mysql_version"
        sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_ldc_managedb.sql | mysql -u root -pwelcome
    fi

fi





