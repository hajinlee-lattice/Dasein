#!/usr/bin/env bash

id -u tomcat &>/dev/null || useradd tomcat
getent group tomcat || groupadd tomcat

APP_DIR="/usr/share/tomcat/webapps/matchapi"

if [ ! -d "${APP_DIR}" ]; then
    mkdir -p ${APP_DIR}
fi

if [ ! -d "${APP_DIR}/manager" ]; then
    cp -r /usr/share/tomcat/webapps/manager ${APP_DIR}
fi

source /etc/profile
rm -rf ${APP_DIR}/*.war
rm -rf /tmp/le-matchapi-rpm