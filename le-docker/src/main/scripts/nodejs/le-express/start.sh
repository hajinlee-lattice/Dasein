#!/usr/bin/env bash

if [ -z "${LE_ENVIRONMENT}" ]; then
    echo "must specify LE_ENVIRONMENT"
    exit 1
fi;

if [ ! -f "/opt/node/app/ENV_VARS" ]; then
    echo "copying ENV_VARS file for LE_ENVIRONMENT=${LE_ENVIRONMENT}"
    cp /opt/node/app/conf/env/${LE_ENVIRONMENT}_aws/ENV_VARS /opt/node/app
fi

if [ "${INSTALL_MODE}" == "EXTERNAL" ]; then
    sed -i "s|{{NODE_APPS}}|leui|g" /opt/node/app/ENV_VARS
    sed -i "s/export ADMIN_HTTPS_PORT=/export ADMIN_HTTPS_PORT=false/g" /opt/node/app/ENV_VARS
elif [ "${INSTALL_MODE}" == "INTERNAL" ]; then
    sed -i "s|{{NODE_APPS}}|leadmin|g" /opt/node/app/ENV_VARS
    sed -i "s/export HTTPS_PORT=/export HTTPS_PORT=false/g" /opt/node/app/ENV_VARS
fi

sed -i "/export LOGGING=/d" /opt/node/app/ENV_VARS

source /opt/node/app/ENV_VARS

npm install debug
node /opt/node/app/app.js
