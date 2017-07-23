#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    echo "Bootstrapping tomcat ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts
    TOMCAT_MAJOR=8
    TOMCAT_VERSION=8.5.15

    sudo rm -rf $CATALINA_HOME
    sudo mkdir -p ${CATALINA_HOME} || true
    sudo chown -R $USER ${CATALINA_HOME} || true

    TOMCAT_TGZ_URL="http://archive.apache.org/dist/tomcat/tomcat-${TOMCAT_MAJOR}/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz"

    if [ ! -f "${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz" ]; then
        wget "${TOMCAT_TGZ_URL}" -O "${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz"
    fi
    rm -rf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION} || true
    tar xzf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}.tar.gz -C ${ARTIFACT_DIR}
    cp -rf ${ARTIFACT_DIR}/apache-tomcat-${TOMCAT_VERSION}/* ${CATALINA_HOME}
    rm -rf ${CATALINA_HOME}/webapps/examples
    rm -rf ${CATALINA_HOME}/webapps/host-manager
    rm -rf ${CATALINA_HOME}/webapps/docs
    rm -rf ${CATALINA_HOME}/webapps/ROOT
fi

cp ${WSHOME}/le-dev/tomcat/server.xml ${CATALINA_HOME}/conf/server.xml
cp ${WSHOME}/le-dev/tomcat/catalina.properties ${CATALINA_HOME}/conf/catalina.properties
cp ${WSHOME}/le-dev/tomcat/tomcat-users.xml ${CATALINA_HOME}/conf/tomcat-users.xml
cp ${WSHOME}/le-dev/tomcat/catalina.sh ${CATALINA_HOME}/bin/catalina.sh

mkdir -p ${CATALINA_HOME}/webapps/ms || true
cp -r ${CATALINA_HOME}/webapps/manager ${CATALINA_HOME}/webapps/ms
chmod -R 775 ${CATALINA_HOME}/webapps/ms/manager

sudo mkdir -p /var/log/ledp || true
sudo chmod a+w /var/log/ledp

sudo mkdir -p /var/log/scoring/mapper || true
sudo chmod a+w /var/log/scoring/mapper

sudo mkdir -p /var/cache/scoringapi || true
sudo chmod a+w /var/cache/scoringapi
