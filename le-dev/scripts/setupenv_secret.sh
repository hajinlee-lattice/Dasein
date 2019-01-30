#!/bin/bash

set -e

set_secret_env() {
    file=$1
    secret=$2
    if [[ -z "${LE_SECRET_KEY}" ]]; then
        echo "export LE_SECRET_KEY=\"$secret\"" >> $file
        echo "setting secret key"
    else
        echo "secret key is already set"
    fi
}

set_salt_hint() {
    file=$1
    salt_hint=$2
    if [[ -z "${LE_SALT_HINT}" ]]; then
        echo "export LE_SALT_HINT=\"$salt_hint\"" >> $file
        echo "setting salt hint"
    else
        echo "salt hint is already set"
    fi
}

UNAME=`uname`
SECRET_KEY="I03TMIIUftFUUI7bV0zFBw=="
SALT_HINT="bi0mpJJNxiYpEka5C6JO4g=="

if [[ "${UNAME}" == 'Darwin' ]]; then
    FILE="$HOME/.bash_profile"
else
    FILE="$HOME/.bashrc"
fi

echo "Setting environment variables to file: $FILE"
echo

set_secret_env $FILE $SECRET_KEY
set_salt_hint $FILE $SALT_HINT

echo
echo "SUCCESS"
echo "Please execute \"source $FILE\" if any of the env is set"
