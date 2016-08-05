#!/usr/bin/env bash

id -u tomcat &>/dev/null || useradd tomcat
getent group tomcat || groupadd tomcat

APP_DIR="${CATALINA_HOME}/webapps/matchapi"

if [ ! -d "${APP_DIR}" ]; then
    mkdir -p ${APP_DIR}
fi

if [ ! -d "${APP_DIR}/manager" ]; then
    cp -r ${CATALINA_HOME}/webapps/manager ${APP_DIR}
fi

source /etc/profile
rm -rf ${APP_DIR}/*.war
rm -rf /tmp/le-matchapi-rpm