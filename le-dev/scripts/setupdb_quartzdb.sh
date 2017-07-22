#!/bin/bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort | head -n 1)" != "$1"; }

UNAME=`uname`
threshold_version=5.6
echo "Setting up QuartzDB"

mysql_version=$(mysql --version | sed 's/.*Distrib //' | cut -d , -f 1) || true
if [ -z "${mysql_version}" ]; then
    mysql_version=5.5
fi
if version_gt ${mysql_version} ${threshold_version}; then
    echo "MySQL version $mysql_version is greater than $threshold_version, replacing DATA by DATA LOCAL"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_quartzdb.sql | sed "s|LOAD DATA INFILE|LOAD DATA LOCAL INFILE|g" | eval $MYSQL_COMMAND
else
    echo "MySQL version $mysql_version"
    sed "s|WSHOME|$WSHOME|g" $WSHOME/le-dev/scripts/setupdb_quartzdb.sql | eval $MYSQL_COMMAND
fi

