#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

UNAME=`uname`
threshold_version=5.6
echo "Setting up ScoringDB"

if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    # Remove alter table drop foreign key statements from the script
    sed -i '' 's/alter table .* drop foreign key .*;//g' $WSHOME/le-scoring/ddl_scoringdb_mysql5innodb.sql
    sed -i '' 's/varchar(65535)/varchar(4000)/g' $WSHOME/le-scoring/ddl_scoringdb_mysql5innodb.sql
else
    echo "You are on ${UNAME}"
    # Remove alter table drop foreign key statements from the script
    sed -i 's/alter table .* drop foreign key .*;//g' $WSHOME/le-scoring/ddl_scoringdb_mysql5innodb.sql
    sed -i 's/varchar(65535)/varchar(4000)/g' $WSHOME/le-scoring/ddl_scoringdb_mysql5innodb.sql
fi

mysql_version=$(mysql --version | sed 's/.*Distrib //' | cut -d , -f 1) || true
if [ -z "${mysql_version}" ]; then
    mysql_version=5.5
fi
if version_gt ${mysql_version} ${threshold_version}; then
    echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_scoringdb.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | eval $MYSQL_COMMAND
else
    echo "MySQL version $mysql_version"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_scoringdb.sql | eval $MYSQL_COMMAND
fi

