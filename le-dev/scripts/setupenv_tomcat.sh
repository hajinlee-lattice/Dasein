#!/usr/bin/env bash

BOOTSTRAP_MODE=$1

if [ "${BOOTSTRAP_MODE}" = "bootstrap" ]; then
    echo "Bootstrapping tomcat ..."
    ARTIFACT_DIR=$WSHOME/le-dev/artifacts

    sudo rm -rf $CATALINA_HOME
    sudo mkdir -p ${CATALINA_HOME} || true
    sudo chown -R $USER ${CATALINA_HOME} || true

    if [ ! -f "${ARTIFACT_DIR}/apache-tomcat-8.5.11.tar.gz" ]; then
        wget https://s3.amazonaws.com/latticeengines-dev/apache-tomcat-8.5.11.tar.gz -O $ARTIFACT_DIR/apache-tomcat-8.5.11.tar.gz
    fi

    tar xzf $ARTIFACT_DIR/apache-tomcat-8.5.11.tar.gz -C $CATALINA_HOME
fi

cp $WSHOME/le-dev/tomcat/server.xml $CATALINA_HOME/conf/server.xml

sudo mkdir -p /var/log/ledp || true
sudo chmod a+w /var/log/ledp

sudo mkdir -p /var/log/scoring/mapper || true
sudo chmod a+w /var/log/scoring/mapper

sudo mkdir -p /var/cache/scoringapi || true
sudo chmod a+w /var/cache/scoringapi