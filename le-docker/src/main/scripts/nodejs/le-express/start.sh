#!/usr/bin/env bash

echo "APP_ROOT=${APP_ROOT}"

if [ -f "/etc/ledp/lattice.crt" ]; then
    echo "Copying /etc/ledp/lattice.crt to /etc/pki/tls/star.lattice.local.crt"
    cp -f /etc/ledp/lattice.crt /etc/pki/tls/star.lattice.local.crt
    cp -f /etc/ledp/lattice.crt /etc/pki/tls/server.crt
fi

if [ -f "/etc/ledp/lattice.key" ]; then
    echo "Copying /etc/ledp/lattice.key /etc/pki/tls/private/private.key"
    cp -f /etc/ledp/lattice.pem /etc/pki/tls/private/private.key
    cp -f /etc/ledp/lattice.pem /etc/pki/tls/server.key
fi
chmod -R 644 /etc/pki/tls

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

cd ${APP_ROOT}
npm install debug
node app.js
