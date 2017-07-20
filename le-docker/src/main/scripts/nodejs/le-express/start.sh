#!/usr/bin/env bash

APP_ROOT=/opt/node/le-ui

if [ -z "${LE_ENVIRONMENT}" ]; then
    echo "must specify LE_ENVIRONMENT"
    exit 1
fi;

if [ ! -f "${APP_ROOT}/ENV_VARS" ]; then
    echo "copying ENV_VARS file for LE_ENVIRONMENT=${LE_ENVIRONMENT}"
    cp ${APP_ROOT}/conf/env/${LE_ENVIRONMENT}/ENV_VARS ${APP_ROOT}
fi

if [ "${INSTALL_MODE}" == "EXTERNAL" ]; then
    sed -i "s|{{NODE_APPS}}|leui|g" ${APP_ROOT}/ENV_VARS
    sed -i "s/export ADMIN_HTTPS_PORT=/export ADMIN_HTTPS_PORT=false/g" ${APP_ROOT}/ENV_VARS
elif [ "${INSTALL_MODE}" == "INTERNAL" ]; then
    sed -i "s|{{NODE_APPS}}|leadmin|g" ${APP_ROOT}/ENV_VARS
    sed -i "s/export HTTPS_PORT=/export HTTPS_PORT=false/g" ${APP_ROOT}/ENV_VARS
else
    sed -i "s|{{NODE_APPS}}|leui,leadmin|g" ${APP_ROOT}/ENV_VARS
fi

sed -i "/export LOGGING=/d" ${APP_ROOT}/ENV_VARS

source ${APP_ROOT}/ENV_VARS

npm install debug
node ${APP_ROOT}/app.js
