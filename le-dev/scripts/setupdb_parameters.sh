#!/bin/bash

db_hostname=localhost
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

echo "Using hostname:$db_hostname user:$db_user password:$db_password"
MYSQL_COMMAND='mysql -h $db_hostname -u $db_user -p$db_password'

