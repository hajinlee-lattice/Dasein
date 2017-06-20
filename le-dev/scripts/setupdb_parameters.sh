#!/bin/bash

db_hostname=127.0.0.1
db_user=root
db_password=welcome

if ! [ -z "$1" ]; then
    db_hostname=$1
fi
if ! [ -z "$2" ]; then
    db_user=$2
fi
if ! [ -z "$3" ]; then
    db_password=$3
fi

UNAME=`uname`

echo "Using hostname:${db_hostname} user:${db_user} password:${db_password}"
if [[ "${UNAME}" == 'Darwin' ]]; then
    echo "You are on Mac"
    MYSQL_COMMAND="mysql -h ${db_hostname} -u ${db_user} -p${db_password} --local-infile=1"
else
    echo "You are on ${UNAME}"
    MYSQL_COMMAND="mysql -h ${db_hostname} -u ${db_user} -p${db_password}"
fi

echo "MYSQL_COMMAND=$MYSQL_COMMAND"
